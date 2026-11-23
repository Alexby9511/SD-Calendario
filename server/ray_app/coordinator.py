from typing import Any, Dict, List, Optional

from .fsm import CalendarFSM

try:
    import ray
except ImportError:
    ray = None


class CoordinatorState:
    """Plain implementation used both locally and from the Ray actor."""

    def __init__(self) -> None:
        self.fsm = CalendarFSM()

    def apply(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a command without raising, matching the replicated adapter."""
        try:
            return self.fsm.apply(command)
        except (KeyError, TypeError, ValueError) as exc:
            return self._rejected(str(exc))
        except Exception as exc:
            return self._rejected(type(exc).__name__)

    def _rejected(self, reason: str) -> Dict[str, Any]:
        return {
            "code": 422,
            "deduplicated": False,
            "applied_index": self.fsm.applied_index,
            "result": {"reason": reason},
        }

    def snapshot(self) -> Dict[str, Any]:
        return self.fsm.read_snapshot()

    def accounts_snapshot(self) -> Dict[str, Dict[str, str]]:
        return self.fsm.accounts_snapshot()

    def availability(
        self,
        users: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        viewer: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.fsm.availability(users, start, end, viewer)

    def conflicts(self, user: str, start: str, end: str, exclude_uid: Optional[str] = None) -> Dict[str, Any]:
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

    def status(self) -> Dict[str, Any]:
        snapshot = self.fsm.read_snapshot()
        return {
            "service": "sd-calendario",
            "middleware": (
                "ray" if ray is not None and ray.is_initialized() else "local"
            ),
            "coordinator_alive": True,
            "applied_index": snapshot["applied_index"],
            "event_count": len(snapshot["events"]),
        }


Coordinator = CoordinatorState
CoordinatorActor = (
    ray.remote(num_cpus=0)(CoordinatorState) if ray is not None else None
)
