"""SQLite persistence for Raft term, vote and log.

Raft requires ``current_term``, ``voted_for`` and the log to survive restarts.
This module keeps that state in one local SQLite database per replica. It is
persistence, not coordination: no other replica reads this file.
"""

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple

from .messages import LogEntry

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS log (
    idx INTEGER PRIMARY KEY,
    term INTEGER NOT NULL,
    command TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshot (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_included_index INTEGER NOT NULL,
    last_included_term INTEGER NOT NULL,
    data BLOB NOT NULL
);
"""


def _encode(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


class Storage(Protocol):
    """Minimal persistence interface used by :class:`RaftNode`."""

    def load_meta(self) -> Dict[str, Any]:
        """Return the stored meta keys, or an empty dict on a fresh database."""

    def save_meta(
        self,
        current_term: int,
        voted_for: Optional[str],
        snapshot_index: int = 0,
        snapshot_term: int = 0,
    ) -> None:
        """Persist the durable election state and snapshot base atomically."""

    def load_log(self) -> List[LogEntry]:
        """Return the stored entries ordered by index."""

    def save_log(
        self,
        entries: Iterable[LogEntry],
        replace: bool = False,
        truncate_from: Optional[int] = None,
    ) -> None:
        """Persist changed entries atomically, optionally replacing a suffix."""

    def save_snapshot(
        self,
        last_included_index: int,
        last_included_term: int,
        data: bytes,
    ) -> None:
        """Persist a snapshot and drop the compacted log prefix atomically."""

    def load_snapshot(self) -> Optional[Tuple[int, int, bytes]]:
        """Return ``(index, term, data)`` or ``None`` when no snapshot exists."""


class SQLiteStorage:
    """Local durable storage for one Raft replica."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(str(self.path), check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        with self._lock:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.executescript(_SCHEMA)
            self._connection.commit()

    def __enter__(self) -> "SQLiteStorage":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def load_meta(self) -> Dict[str, Any]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT key, value FROM meta"
            ).fetchall()
        return {row["key"]: json.loads(row["value"]) for row in rows}

    def save_meta(
        self,
        current_term: int,
        voted_for: Optional[str],
        snapshot_index: int = 0,
        snapshot_term: int = 0,
    ) -> None:
        values = {
            "current_term": current_term,
            "voted_for": voted_for,
            "snapshot_index": snapshot_index,
            "snapshot_term": snapshot_term,
        }
        with self._lock, self._connection:
            self._connection.executemany(
                "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
                [(key, _encode(value)) for key, value in values.items()],
            )

    def load_log(self) -> List[LogEntry]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT idx, term, command FROM log ORDER BY idx"
            ).fetchall()
        return [
            LogEntry(
                index=row["idx"],
                term=row["term"],
                command=json.loads(row["command"]),
            )
            for row in rows
        ]

    def save_log(
        self,
        entries: Iterable[LogEntry],
        replace: bool = False,
        truncate_from: Optional[int] = None,
    ) -> None:
        if replace and truncate_from is not None:
            raise ValueError("raft: cannot replace and truncate the log together")
        rows = [
            (entry.index, entry.term, _encode(entry.command)) for entry in entries
        ]
        with self._lock, self._connection:
            if replace:
                self._connection.execute("DELETE FROM log")
            elif truncate_from is not None:
                self._connection.execute(
                    "DELETE FROM log WHERE idx >= ?", (truncate_from,)
                )
            self._connection.executemany(
                "INSERT OR REPLACE INTO log(idx, term, command) VALUES (?, ?, ?)",
                rows,
            )

    def save_snapshot(
        self,
        last_included_index: int,
        last_included_term: int,
        data: bytes,
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR REPLACE INTO snapshot"
                "(id, last_included_index, last_included_term, data) "
                "VALUES (1, ?, ?, ?)",
                (last_included_index, last_included_term, data),
            )
            self._connection.executemany(
                "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
                [
                    ("snapshot_index", _encode(last_included_index)),
                    ("snapshot_term", _encode(last_included_term)),
                ],
            )
            self._connection.execute(
                "DELETE FROM log WHERE idx <= ?", (last_included_index,)
            )

    def load_snapshot(self) -> Optional[Tuple[int, int, bytes]]:
        with self._lock:
            row = self._connection.execute(
                "SELECT last_included_index, last_included_term, data "
                "FROM snapshot WHERE id = 1"
            ).fetchone()
        if row is None:
            return None
        return (
            int(row["last_included_index"]),
            int(row["last_included_term"]),
            bytes(row["data"]),
        )
