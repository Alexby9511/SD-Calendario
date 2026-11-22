"""Ray actor wrapper for one Raft replica.

The actor owns the state machine, the SQLite storage and the ``RaftNode``.
Ray serializes actor methods, so the node stays single-threaded. Incoming
RPC handlers never send RPCs themselves; only ``tick``, ``start_election``,
``propose`` and ``send_heartbeats`` do. That keeps request/response cycles
from deadlocking two actors.
"""

import json
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol, Tuple

from .consensus.clock import RealClock
from .consensus.node import RaftNode
from .consensus.storage import SQLiteStorage
from .raft_placement import placement_options
from .raft_transport import DEFAULT_NAMESPACE, RayTransport, actor_name
from .state import CalendarState

DEFAULT_TRANSPORT_TIMEOUT = 0.1
DEFAULT_ELECTION_TIMEOUT = (0.5, 1.5)
DEFAULT_HEARTBEAT_INTERVAL = 0.05

try:
    import ray
except ImportError:
    ray = None


class ReplicatedState(Protocol):
    """State machine owned by a replica."""

    def apply(self, command: Dict[str, Any]) -> Any:
        """Apply a committed command and return its result."""

    def snapshot(self) -> bytes:
        """Serialize the whole state."""

    def restore(self, data: bytes) -> None:
        """Replace the whole state from a snapshot."""

    def availability(
        self,
        users: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return the busy windows of the requested users."""

    def conflicts(
        self,
        user: str,
        start: str,
        end: str,
        exclude_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return the events of ``user`` that overlap the given window."""

    def sync(self, since: int = 0, limit: int = 100) -> Dict[str, Any]:
        """Return the applied entries after the given cursor."""


class RaftReplica:
    """Plain replica implementation; wrapped by ``RaftActor`` in Ray."""

    def __init__(
        self,
        node_id: str,
        peers: Iterable[str],
        db_path: str,
        state_factory: Callable[[], ReplicatedState] = CalendarState,
        namespace: str = DEFAULT_NAMESPACE,
        transport_timeout: float = DEFAULT_TRANSPORT_TIMEOUT,
        election_timeout: Tuple[float, float] = DEFAULT_ELECTION_TIMEOUT,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        placement_key: Optional[str] = None,
    ) -> None:
        self.state = state_factory()
        self.storage = SQLiteStorage(db_path)
        self.placement_key = placement_key
        self.node = RaftNode(
            node_id,
            peers,
            RayTransport(namespace=namespace, timeout=transport_timeout),
            clock=RealClock(),
            election_timeout=election_timeout,
            heartbeat_interval=heartbeat_interval,
            apply_command=self.state.apply,
            storage=self.storage,
            snapshot_provider=self.state.snapshot,
            snapshot_restorer=self.state.restore,
        )

    def handle_message(self, message: Any) -> Any:
        return self.node.handle_message(message)

    def tick(self) -> None:
        self.node.tick()

    def start_election(self) -> None:
        self.node.start_election()

    def send_heartbeats(self) -> None:
        self.node.send_heartbeats()

    def propose(self, command: Dict[str, Any]) -> Any:
        return self.node.propose(command)

    def take_snapshot(self) -> bool:
        return self.node.take_snapshot()

    def snapshot(self) -> bytes:
        return self.state.snapshot()

    def state_snapshot(self) -> Dict[str, Any]:
        return json.loads(self.state.snapshot().decode("utf-8"))

    def availability(
        self,
        users: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.state.availability(users, start, end)

    def conflicts(
        self,
        user: str,
        start: str,
        end: str,
        exclude_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.state.conflicts(user, start, end, exclude_uid)

    def sync(self, since: int = 0, limit: int = 100) -> Dict[str, Any]:
        return self.state.sync(since, limit)

    def status(self) -> Dict[str, Any]:
        status = self.node.status()
        status["placement_key"] = self.placement_key
        if ray is not None:
            status["ray_node_id"] = ray.get_runtime_context().get_node_id()
        return status

    def close(self) -> None:
        self.storage.close()


RaftActor = ray.remote(num_cpus=0)(RaftReplica) if ray is not None else None


def start_raft_cluster(
    node_ids: Iterable[str],
    db_paths: Dict[str, str],
    state_factory: Callable[[], ReplicatedState] = CalendarState,
    namespace: str = DEFAULT_NAMESPACE,
    membership: Optional[Iterable[str]] = None,
    detached: bool = True,
    get_if_exists: bool = True,
    node_assignments: Optional[Dict[str, str]] = None,
    skip_unassigned: bool = False,
) -> Dict[str, Any]:
    """Start one actor per node and return ``node_id -> actor handle``.

    ``node_ids`` selects which actors to start; ``membership`` is the full
    replica set used to build each peer list. That allows a test or a rolling
    deployment to start a subset while the rest of the cluster knows about
    every member.

    Detached actors outlive the process that created them, so the replicas
    keep serving when an API process dies. ``get_if_exists`` lets a second
    API process attach to the replicas that already exist. When
    ``node_assignments`` is provided, each actor is pinned to its assigned
    Ray node so that its SQLite database remains node-local.

    ``skip_unassigned`` leaves out the replicas without an assignment: the
    supervisor recreates them on their original node instead of placing them
    somewhere else.
    """
    if ray is None or RaftActor is None:
        raise RuntimeError("raft: ray is not installed")
    started = list(node_ids)
    members = list(membership) if membership is not None else started
    assignments = dict(node_assignments or {})
    targets = [
        assignments[node_id]
        for node_id in started
        if assignments.get(node_id)
    ]
    if len(set(targets)) != len(targets):
        raise ValueError("raft: replica placement requires distinct Ray nodes")
    actors: Dict[str, Any] = {}
    for node_id in started:
        target_node = assignments.get(node_id)
        if skip_unassigned and not target_node:
            continue
        peers = [other for other in members if other != node_id]
        options: Dict[str, Any] = {
            "name": actor_name(node_id),
            "namespace": namespace,
            "get_if_exists": get_if_exists,
        }
        if detached:
            options["lifetime"] = "detached"
        if target_node:
            options.update(placement_options(target_node))
        actors[node_id] = RaftActor.options(**options).remote(
            node_id,
            peers,
            db_paths[node_id],
            state_factory,
            namespace,
            DEFAULT_TRANSPORT_TIMEOUT,
            DEFAULT_ELECTION_TIMEOUT,
            DEFAULT_HEARTBEAT_INTERVAL,
            target_node,
        )
    if actors:
        # Wait for every constructor to finish before the caller starts
        # elections; otherwise the first RPC can race actor creation.
        ray.get([actor.status.remote() for actor in actors.values()])
    return actors


def stop_raft_cluster(actors: Dict[str, Any]) -> None:
    """Close storage and kill every actor in ``actors``."""
    if ray is None:
        return
    for actor in actors.values():
        try:
            ray.get(actor.close.remote(), timeout=2.0)
        except Exception:
            pass
        try:
            ray.kill(actor)
        except Exception:
            pass
