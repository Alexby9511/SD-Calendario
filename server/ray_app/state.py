"""State-machine adapter used by Raft replicas.

``CalendarFSM`` snapshots are dictionaries; Raft snapshots are opaque bytes.
This adapter owns the encoding and keeps the consensus layer independent from
the calendar domain.
"""

import json
from typing import Any, Dict, List, Optional

from pkg.protocol.canonical import canonical_bytes

from .fsm import CalendarFSM


class CalendarState:
    """CalendarFSM with the ``apply``/``snapshot``/``restore`` interface."""

    def __init__(self) -> None:
        self.fsm = CalendarFSM()

    def apply(self, command: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return self.fsm.apply(command)
        except (KeyError, TypeError, ValueError) as exc:
            return {
                "code": 422,
                "deduplicated": False,
                "applied_index": self.fsm.applied_index,
                "result": {"reason": str(exc)},
            }

    def snapshot(self) -> bytes:
        return canonical_bytes(self.fsm.snapshot())

    def restore(self, data: bytes) -> None:
        self.fsm = CalendarFSM.from_snapshot(
            json.loads(data.decode("utf-8"))
        )

    def availability(
        self,
        users: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.fsm.availability(users, start, end)

    def conflicts(
        self,
        user: str,
        start: str,
        end: str,
        exclude_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.fsm.conflicts(user, start, end, exclude_uid)

    def sync(
        self, since: int = 0, limit: int = 100, user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        if user_id is not None:
            return self.fsm.sync_for(user_id, since, limit)
        return self.fsm.sync(since, limit)
