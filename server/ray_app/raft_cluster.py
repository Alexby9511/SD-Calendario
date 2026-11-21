"""Cluster-level operations over the Raft replicas.

The API talks to a cluster, not to a single replica: it needs to find the
current leader, forward writes to it and aggregate the state of every replica.
"""

from typing import Any, Dict, List, Optional, Tuple

from .consensus.node import NotLeaderError
from .consensus.transport import TransportError
from .raft_transport import DEFAULT_ACTOR_PREFIX, DEFAULT_NAMESPACE, actor_name

try:
    import ray
except ImportError:
    ray = None

_RAY_ERRORS: Tuple[type, ...] = (
    (ray.exceptions.RayError,) if ray is not None else ()
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
        return {
            "service": "sd-calendario",
            "middleware": "ray",
            "leader_id": self.leader(statuses),
            "replicas": statuses,
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

    def snapshot(self) -> Dict[str, Any]:
        """Return the state machine snapshot held by the leader."""
        return self.call_leader("state_snapshot")

    def availability(
        self,
        users: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.call_leader("availability", users, start, end)

    def conflicts(
        self,
        user: str,
        start: str,
        end: str,
        exclude_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.call_leader("conflicts", user, start, end, exclude_uid)

    def sync(self, since: int = 0, limit: int = 100) -> Dict[str, Any]:
        return self.call_leader("sync", since, limit)
