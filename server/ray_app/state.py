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
        """Apply a command, never raising: the replicated log must stay total.

        An exception escaping here aborts ``RaftNode._apply_committed`` with the
        entry still unapplied, so every later commit re-raises and consensus is
        permanently wedged on all replicas. Malformed commands are recorded as
        rejections instead.
        """
        try:
            return self.fsm.apply(command)
        except (KeyError, TypeError, ValueError) as exc:
            return self._rejected(str(exc))
        except Exception as exc:
            # Defence in depth: keep the exception class (deterministic across
            # replicas) but not its message, which may not be.
            return self._rejected(type(exc).__name__)

    def _rejected(self, reason: str) -> Dict[str, Any]:
        return {
            "code": 422,
            "deduplicated": False,
            "applied_index": self.fsm.applied_index,
            "result": {"reason": reason},
        }

    def snapshot(self) -> bytes:
        return canonical_bytes(self.fsm.snapshot())

    def read_snapshot(self) -> Dict[str, Any]:
        """Return the domain projection without its sync history."""
        return self.fsm.read_snapshot()

    def accounts_snapshot(self) -> Dict[str, Dict[str, str]]:
        return self.fsm.accounts_snapshot()

    def restore(self, data: bytes) -> None:
        self.fsm = CalendarFSM.from_snapshot(
            json.loads(data.decode("utf-8"))
        )

    def availability(
        self,
        users: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        viewer: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.fsm.availability(users, start, end, viewer)

    def conflicts(
        self,
        user: str,
        start: str,
        end: str,
        exclude_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.fsm.conflicts(user, start, end, exclude_uid)

    def sync(
        self,
        since: int = 0,
        limit: int = 100,
        user_id: Optional[str] = None,
        include_snapshot: bool = True,
    ) -> Dict[str, Any]:
        if user_id is not None:
            return self.fsm.sync_for(user_id, since, limit, include_snapshot)
        return self.fsm.sync(since, limit)

    def has_expired_events(self, now: str) -> bool:
        return self.fsm.has_expired_events(now)
