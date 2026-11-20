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
        try:
            return self.fsm.apply(command)
        except (KeyError, TypeError, ValueError) as exc:
            return {
                "code": 422,
                "deduplicated": False,
                "applied_index": self.fsm.applied_index,
                "result": {"reason": str(exc)},
            }

    def snapshot(self) -> Dict[str, Any]:
        return self.fsm.snapshot()

    def availability(
        self, users: Optional[List[str]] = None, start: Optional[str] = None, end: Optional[str] = None
    ) -> Dict[str, Any]:
        return self.fsm.availability(users, start, end)

    def conflicts(self, user: str, start: str, end: str, exclude_uid: Optional[str] = None) -> Dict[str, Any]:
        return self.fsm.conflicts(user, start, end, exclude_uid)

    def sync(self, since: int = 0, limit: int = 100) -> Dict[str, Any]:
        return self.fsm.sync(since, limit)

    def status(self) -> Dict[str, Any]:
        snapshot = self.snapshot()
        return {
            "service": "sd-calendario",
            "middleware": "ray",
            "coordinator_alive": True,
            "applied_index": snapshot["applied_index"],
            "event_count": len(snapshot["events"]),
        }


Coordinator = CoordinatorState
CoordinatorActor = (
    ray.remote(num_cpus=0)(CoordinatorState) if ray is not None else None
)
