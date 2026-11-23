"""Cluster-level operations over the Raft replicas.

The API talks to a cluster, not to a single replica: it needs to find the
current leader, forward writes to it and aggregate the state of every replica.
"""

from typing import Any, Dict, List, Optional, Tuple

from .consensus.node import NotLeaderError
from .consensus.transport import TransportError
from .fsm import CalendarFSM, MAX_SYNC_LIMIT
from .raft_transport import DEFAULT_ACTOR_PREFIX, DEFAULT_NAMESPACE, actor_name

try:
    import ray
except ImportError:
    ray = None

_RAY_ERRORS: Tuple[type, ...] = (
    (ray.exceptions.RayError,) if ray is not None else ()
)


def _run_calendar_query(
    snapshot: Dict[str, Any], method: str, args: Tuple[Any, ...]
) -> Dict[str, Any]:
    """Run a read-only calendar query against an immutable replica snapshot."""
    state = CalendarFSM.from_snapshot(snapshot)
    if method not in ("availability", "conflicts", "sync", "sync_for"):
        raise ValueError(f"raft: unsupported calendar query {method!r}")
    return getattr(state, method)(*args)


calendar_query_task = (
    ray.remote(num_cpus=0.1)(_run_calendar_query) if ray is not None else None
)


class NoLeaderError(Exception):
    """Raised when no replica currently acts as leader."""


class RaftCluster:
    """Handle to the set of Raft replicas and their current leader."""

    def __init__(
        self,
        replicas: Any = None,
        namespace: str = DEFAULT_NAMESPACE,
        timeout: float = 3.0,
        actor_prefix: str = DEFAULT_ACTOR_PREFIX,
    ) -> None:
        if isinstance(replicas, dict):
            self.node_ids: List[str] = list(replicas)
        elif replicas is None:
            self.node_ids = []
        else:
            self.node_ids = list(replicas)
        self.namespace = namespace
        self.timeout = timeout
        self.actor_prefix = actor_prefix

    def actor(self, node_id: str) -> Any:
        """Resolve the actor of a replica by name.

        The handle is looked up on every call, so the cluster follows a
        replica that the supervisor recreated after a crash.
        """
        return ray.get_actor(
            actor_name(node_id, self.actor_prefix),
            namespace=self.namespace,
        )

    def statuses(self) -> Dict[str, Dict[str, Any]]:
        """Return the status of every reachable replica."""
        result: Dict[str, Dict[str, Any]] = {}
        for node_id in self.node_ids:
            try:
                actor = self.actor(node_id)
                result[node_id] = ray.get(
                    actor.status.remote(), timeout=self.timeout
                )
            except Exception:
                continue
        return result

    def leader(self, statuses: Optional[Dict[str, Dict[str, Any]]] = None) -> Optional[str]:
        """Return the node id of the leader with the highest term."""
        known = self.statuses() if statuses is None else statuses
        candidates = [
            (int(status.get("term", 0)), node_id)
            for node_id, status in known.items()
            if status.get("role") == "leader"
        ]
        if not candidates:
            return None
        return max(candidates)[1]

    def propose(self, command: Dict[str, Any], attempts: int = 3) -> Any:
        """Forward a command to the leader and return its result.

        Returns ``None`` when the leader could not commit the command, which
        means there was no quorum. Raises :class:`NoLeaderError` when no
        replica is willing to lead.
        """
        last_error: Optional[Exception] = None
        for _ in range(attempts):
            leader_id = self.leader()
            if leader_id is None:
                raise NoLeaderError("raft: no leader available")
            try:
                return ray.get(
                    self.actor(leader_id).propose.remote(command),
                    timeout=self.timeout,
                )
            except (NotLeaderError, TransportError) + _RAY_ERRORS as exc:
                last_error = exc
                continue
        raise NoLeaderError("raft: leader unavailable") from last_error

    def status(self) -> Dict[str, Any]:
        """Aggregate the state of every replica."""
        statuses = self.statuses()
        quorum_size = len(self.node_ids) // 2 + 1
        alive_replicas = len(statuses)
        leader_id = self.leader(statuses)
        return {
            "service": "sd-calendario",
            "middleware": "ray",
            "leader_id": leader_id,
            "replicas": statuses,
            "expected_replicas": len(self.node_ids),
            "alive_replicas": alive_replicas,
            "quorum_size": quorum_size,
            "quorum_available": alive_replicas >= quorum_size,
            "applied_index": (
                statuses.get(leader_id, {}).get("last_applied", 0)
                if leader_id is not None
                else max(
                    (status.get("last_applied", 0) for status in statuses.values()),
                    default=0,
                )
            ),
        }

    def call_leader(self, method: str, *args: Any) -> Any:
        """Call a read-only method on the current leader."""
        leader_id = self.leader()
        if leader_id is None:
            raise NoLeaderError("raft: no leader available")
        actor = self.actor(leader_id)
        return ray.get(
            getattr(actor, method).remote(*args),
            timeout=self.timeout,
        )

    def snapshot(self, include_log: bool = False) -> Dict[str, Any]:
        """Return the state machine snapshot held by the leader."""
        return self.call_leader("state_snapshot", include_log)

    def availability(
        self,
        users: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        viewer: Optional[str] = None,
    ) -> Dict[str, Any]:
        snapshot = self.snapshot()
        selected = users if users else sorted(
            set(snapshot.get("busy", {}))
            | {
                user
                for event in snapshot.get("events", {}).values()
                for user in [event["organizer"], *event["participants"]]
            }
        )
        if not selected:
            return {"users": {}, "as_of": snapshot["applied_index"]}

        # Bound fan-out while keeping independent user projections concurrent.
        batch_count = min(4, len(selected))
        batch_size = (len(selected) + batch_count - 1) // batch_count
        batches = [
            selected[index : index + batch_size]
            for index in range(0, len(selected), batch_size)
        ]
        snapshot_ref = ray.put(snapshot)
        pending = [
            calendar_query_task.remote(
                snapshot_ref, "availability", (batch, start, end, viewer)
            )
            for batch in batches
        ]
        result: Dict[str, Any] = {
            "users": {},
            "as_of": snapshot["applied_index"],
        }
        while pending:
            ready, pending = ray.wait(
                pending, num_returns=1, timeout=self.timeout
            )
            if not ready:
                raise TimeoutError("raft: availability query task timed out")
            partial = ray.get(ready[0], timeout=self.timeout)
            result["users"].update(partial["users"])
        return result

    def conflicts(
        self,
        user: str,
        start: str,
        end: str,
        exclude_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        snapshot_ref = ray.put(self.snapshot())
        return ray.get(
            calendar_query_task.remote(
                snapshot_ref, "conflicts", (user, start, end, exclude_uid)
            ),
            timeout=self.timeout,
        )

    def sync(
        self,
        since: int = 0,
        limit: int = 100,
        user_id: Optional[str] = None,
        include_snapshot: bool = True,
    ) -> Dict[str, Any]:
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or limit <= 0
            or limit > MAX_SYNC_LIMIT
        ):
            raise ValueError("invalid_sync_limit")
        if not isinstance(since, int) or isinstance(since, bool) or since < 0:
            raise ValueError("invalid_sync_cursor")
        return self.call_leader(
            "sync", since, limit, user_id, include_snapshot
        )
