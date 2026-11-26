import json
import sqlite3
from typing import Any, Callable, Dict, Iterable, List, Optional

# Status codes that describe a command the server will keep rejecting: the
# payload or the identity is wrong, so retrying it cannot help.
PERMANENT_REJECTION_CODES = frozenset({400, 403, 404, 410, 422})


class Outbox:
    """Durable command outbox and sync inbox for an offline-first client."""

    def __init__(self, path: str = "outbox.db") -> None:
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS outbox (
                msg_id TEXT PRIMARY KEY,
                uid TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                payload TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'PENDING',
                attempts INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        columns = {
            row[1] for row in self.connection.execute("PRAGMA table_info(outbox)")
        }
        if "last_error" not in columns:
            self.connection.execute(
                "ALTER TABLE outbox ADD COLUMN last_error TEXT"
            )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS sync_state (key TEXT PRIMARY KEY, value INTEGER NOT NULL)"
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS remote_events (
                uid TEXT PRIMARY KEY,
                event TEXT NOT NULL,
                attendance TEXT NOT NULL
            )
            """
        )
        self.connection.commit()

    def enqueue(self, command: Dict[str, Any]) -> None:
        self._validate_command(command)
        encoded = self._encode(command)
        row = self.connection.execute(
            "SELECT payload FROM outbox WHERE msg_id=?", (command["msg_id"],)
        ).fetchone()
        if row is not None:
            if row[0] != encoded:
                raise ValueError("outbox msg_id is already used by another command")
            return
        self.connection.execute(
            """
            INSERT INTO outbox(msg_id, uid, sequence, payload)
            VALUES (?, ?, ?, ?)
            """,
            (
                command["msg_id"],
                command["uid"],
                command["sequence"],
                encoded,
            ),
        )
        self.connection.commit()

    def pending(self) -> List[Dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT msg_id, uid, sequence, payload, attempts "
            "FROM outbox WHERE state='PENDING' ORDER BY rowid"
        ).fetchall()
        items = []
        for row in rows:
            command = json.loads(row[3])
            self._validate_command(command)
            items.append(
                {
                    "msg_id": row[0],
                    "uid": row[1],
                    "sequence": row[2],
                    "command": command,
                    "attempts": row[4],
                }
            )
        return items

    def send_pending(
        self, send: Callable[[Dict[str, Any]], Dict[str, Any]]
    ) -> int:
        """Send pending commands in order, preserving failed commands for retry."""
        sent = 0
        for item in self.pending():
            command = item["command"]
            self.mark_attempt(item["msg_id"])
            response = send(command)
            if not isinstance(response, dict):
                raise TypeError("outbox sender must return a response object")
            if response.get("code") == 200:
                self.mark_sent(item["msg_id"])
                sent += 1
                continue
            if response.get("code") == 409:
                self.mark_conflict(item["msg_id"], response)
            elif response.get("code") in PERMANENT_REJECTION_CODES:
                # Retrying this command can never succeed, so park it instead
                # of leaving it at the head of the queue forever.
                self.mark_rejected(item["msg_id"], response)
            raise OutboxDeliveryError(item["msg_id"], response)
        return sent

    def mark_attempt(self, msg_id: str) -> None:
        cursor = self.connection.execute(
            "UPDATE outbox SET attempts=attempts+1 "
            "WHERE msg_id=? AND state='PENDING'",
            (msg_id,),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"outbox pending command not found: {msg_id}")
        self.connection.commit()

    def mark_sent(self, msg_id: str) -> None:
        cursor = self.connection.execute(
            "UPDATE outbox SET state='SENT' "
            "WHERE msg_id=? AND state='PENDING'",
            (msg_id,),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"outbox pending command not found: {msg_id}")
        self.connection.commit()

    def mark_conflict(self, msg_id: str, response: Dict[str, Any]) -> None:
        cursor = self.connection.execute(
            "UPDATE outbox SET state='CONFLICT', last_error=? "
            "WHERE msg_id=? AND state='PENDING'",
            (
                self._encode({"command_error": response}),
                msg_id,
            ),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"outbox pending command not found: {msg_id}")
        self.connection.commit()

    def mark_rejected(self, msg_id: str, response: Dict[str, Any]) -> None:
        cursor = self.connection.execute(
            "UPDATE outbox SET state='REJECTED', last_error=? "
            "WHERE msg_id=? AND state='PENDING'",
            (
                self._encode({"command_error": response}),
                msg_id,
            ),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"outbox pending command not found: {msg_id}")
        self.connection.commit()

    def conflicts(self) -> List[Dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT msg_id, uid, sequence, payload, attempts, last_error "
            "FROM outbox WHERE state='CONFLICT' ORDER BY rowid"
        ).fetchall()
        return [
            {
                "msg_id": row[0],
                "uid": row[1],
                "sequence": row[2],
                "command": json.loads(row[3]),
                "attempts": row[4],
                "error": json.loads(row[5]) if row[5] else None,
            }
            for row in rows
        ]

    def rejected(self) -> List[Dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT msg_id, uid, sequence, payload, attempts, last_error "
            "FROM outbox WHERE state='REJECTED' ORDER BY rowid"
        ).fetchall()
        return [
            {
                "msg_id": row[0],
                "uid": row[1],
                "sequence": row[2],
                "command": json.loads(row[3]),
                "attempts": row[4],
                "error": json.loads(row[5]) if row[5] else None,
            }
            for row in rows
        ]

    def cursor(self) -> int:
        row = self.connection.execute(
            "SELECT value FROM sync_state WHERE key='calendar'"
        ).fetchone()
        return int(row[0]) if row else 0

    def set_cursor(self, value: int) -> None:
        if value < self.cursor():
            raise ValueError("sync cursor cannot move backwards")
        self.connection.execute(
            "INSERT INTO sync_state(key, value) VALUES ('calendar', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (value,),
        )
        self.connection.commit()

    def reconcile(
        self,
        entries: Iterable[Dict],
        next_cursor: Optional[int] = None,
        snapshot: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Atomically persist a sync page and advance across filtered records."""
        cursor = self.cursor()
        page = list(entries)
        indexes = [self._entry_index(entry) for entry in page]
        if any(left >= right for left, right in zip(indexes, indexes[1:])):
            raise ValueError("sync entries are not in increasing order")

        target = indexes[-1] if indexes else cursor
        if next_cursor is not None:
            target = int(next_cursor)
            if target < (indexes[-1] if indexes else cursor):
                raise ValueError("sync cursor precedes returned entries")
        if target < cursor:
            raise ValueError("sync cursor cannot move backwards")
        if snapshot is not None:
            if not isinstance(snapshot, dict) or not isinstance(
                snapshot.get("events"), dict
            ):
                raise ValueError("sync snapshot must contain an events object")
            as_of = snapshot.get("applied_index")
            if not isinstance(as_of, int) or isinstance(as_of, bool) or as_of < target:
                raise ValueError("sync snapshot index precedes its cursor")
        if next_cursor is None and indexes:
            expected = cursor + 1
            for index in indexes:
                if index <= cursor:
                    continue
                if index != expected:
                    raise ValueError("sync entries are not contiguous")
                expected += 1

        with self.connection:
            for entry in page:
                event = entry.get("event")
                if isinstance(event, dict) and isinstance(event.get("uid"), str):
                    self.connection.execute(
                        """
                        INSERT INTO remote_events(uid, event, attendance)
                        VALUES (?, ?, ?)
                        ON CONFLICT(uid) DO UPDATE SET
                            event=excluded.event,
                            attendance=excluded.attendance
                        """,
                        (
                            event["uid"],
                            self._encode(event),
                            self._encode(entry.get("attendance", {})),
                        ),
                    )
            if snapshot is not None:
                self.connection.execute("DELETE FROM remote_events")
                attendance = snapshot.get("attendance", {})
                if not isinstance(attendance, dict):
                    raise ValueError("sync snapshot attendance must be an object")
                for uid, event in snapshot["events"].items():
                    if not isinstance(uid, str) or not isinstance(event, dict):
                        raise ValueError("sync snapshot contains an invalid event")
                    self.connection.execute(
                        """
                        INSERT INTO remote_events(uid, event, attendance)
                        VALUES (?, ?, ?)
                        """,
                        (
                            uid,
                            self._encode(event),
                            self._encode(attendance.get(uid, {})),
                        ),
                    )
            self.connection.execute(
                "INSERT INTO sync_state(key, value) VALUES ('calendar', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (target,),
            )
            self.connection.execute(
                "INSERT INTO sync_state(key, value) VALUES ('as_of', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (snapshot["applied_index"] if snapshot is not None else target,),
            )
        return target

    def synced_snapshot(self) -> Dict[str, Any]:
        rows = self.connection.execute(
            "SELECT uid, event, attendance FROM remote_events ORDER BY uid"
        ).fetchall()
        as_of = self.connection.execute(
            "SELECT value FROM sync_state WHERE key='as_of'"
        ).fetchone()
        return {
            "events": {row[0]: json.loads(row[1]) for row in rows},
            "attendance": {row[0]: json.loads(row[2]) for row in rows},
            "applied_index": int(as_of[0]) if as_of else 0,
        }

    def close(self) -> None:
        self.connection.close()

    @staticmethod
    def _encode(value: Dict[str, Any]) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _entry_index(entry: Dict[str, Any]) -> int:
        try:
            index = entry["index"]
        except (KeyError, TypeError) as exc:
            raise ValueError("sync entry is missing its index") from exc
        if not isinstance(index, int) or isinstance(index, bool) or index < 1:
            raise ValueError("sync entry index must be a positive integer")
        return index

    @staticmethod
    def _validate_command(command: Dict[str, Any]) -> None:
        required = {
            "deployment_id",
            "group_id",
            "msg_id",
            "type",
            "uid",
            "sender_id",
            "key_id",
            "sequence",
            "seen_sequence",
            "client_sent_at",
            "payload",
        }
        if not isinstance(command, dict) or required - set(command):
            raise ValueError("outbox requires a complete calendar command")
        if not isinstance(command["msg_id"], str) or not command["msg_id"].strip():
            raise ValueError("outbox command requires a non-empty msg_id")
        if (
            not isinstance(command["uid"], str)
            or not command["uid"].strip()
            or not isinstance(command["sequence"], int)
            or isinstance(command["sequence"], bool)
            or command["sequence"] < 1
        ):
            raise ValueError("outbox command has invalid event identity or sequence")


class OutboxDeliveryError(RuntimeError):
    """A server rejected a pending command; the caller must reconcile it."""

    def __init__(self, msg_id: str, response: Dict[str, Any]) -> None:
        super().__init__(
            f"outbox command {msg_id} was not accepted: {response!r}"
        )
        self.msg_id = msg_id
        self.response = response
