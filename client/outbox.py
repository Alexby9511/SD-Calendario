import json
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Optional


class Outbox:
    """SQLite outbox: pending, sent and discarded messages by msg_id."""

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
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS sync_state (key TEXT PRIMARY KEY, value INTEGER NOT NULL)"
        )
        self.connection.commit()

    def enqueue(self, msg_id: str, uid: str, sequence: int, payload: Dict) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO outbox(msg_id, uid, sequence, payload) VALUES (?, ?, ?, ?)",
            (msg_id, uid, sequence, json.dumps(payload, sort_keys=True, separators=(",", ":"))),
        )
        self.connection.commit()

    def pending(self) -> Iterable[Dict]:
        rows = self.connection.execute(
            "SELECT msg_id, uid, sequence, payload, attempts "
            "FROM outbox WHERE state='PENDING' ORDER BY rowid"
        ).fetchall()
        return [
            {
                "msg_id": row[0],
                "uid": row[1],
                "sequence": row[2],
                "payload": json.loads(row[3]),
                "attempts": row[4],
            }
            for row in rows
        ]

    def mark_attempt(self, msg_id: str) -> None:
        self.connection.execute(
            "UPDATE outbox SET attempts=attempts+1 WHERE msg_id=?", (msg_id,)
        )
        self.connection.commit()

    def mark_sent(self, msg_id: str) -> None:
        self.connection.execute(
            "UPDATE outbox SET state='SENT' WHERE msg_id=?", (msg_id,)
        )
        self.connection.commit()

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

    def reconcile(self, entries: Iterable[Dict]) -> int:
        """Advance the cursor only after a contiguous server page is applied."""
        cursor = self.cursor()
        for entry in entries:
            index = int(entry["index"])
            if index != cursor + 1:
                raise ValueError("sync entries are not contiguous")
            cursor = index
        self.set_cursor(cursor)
        return cursor

    def close(self) -> None:
        self.connection.close()
