"""Ray actor wrapper for one Raft replica.

The actor owns the state machine, the SQLite storage and the ``RaftNode``.
Ray serializes actor methods, so the node stays single-threaded. Incoming
RPC handlers never send RPCs themselves; only ``tick``, ``start_election``,
``propose`` and ``send_heartbeats`` do. That keeps request/response cycles
from deadlocking two actors.
"""

from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol

from .consensus.clock import RealClock
from .consensus.node import RaftNode
from .consensus.storage import SQLiteStorage
from .raft_transport import DEFAULT_NAMESPACE, RayTransport, actor_name
from .state import CalendarState

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


class RaftReplica:
    """Plain replica implementation; wrapped by ``RaftActor`` in Ray."""

    def __init__(
        self,
        node_id: str,
        peers: Iterable[str],
        db_path: str,
        state_factory: Callable[[], ReplicatedState] = CalendarState,
        namespace: str = DEFAULT_NAMESPACE,
        transport_timeout: float = 0.5,
    ) -> None:
        self.state = state_factory()
        self.storage = SQLiteStorage(db_path)
        self.node = RaftNode(
            node_id,
            peers,
            RayTransport(namespace=namespace, timeout=transport_timeout),
            clock=RealClock(),
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

    def status(self) -> Dict[str, Any]:
        return self.node.status()

    def close(self) -> None:
        self.storage.close()


RaftActor = ray.remote(num_cpus=0)(RaftReplica) if ray is not None else None


def start_raft_cluster(
    node_ids: Iterable[str],
    db_paths: Dict[str, str],
    state_factory: Callable[[], ReplicatedState] = CalendarState,
    namespace: str = DEFAULT_NAMESPACE,
    membership: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Start one actor per node and return ``node_id -> actor handle``.

    ``node_ids`` selects which actors to start; ``membership`` is the full
    replica set used to build each peer list. That allows a test or a rolling
    deployment to start a subset while the rest of the cluster knows about
    every member.
    """
    if ray is None or RaftActor is None:
        raise RuntimeError("raft: ray is not installed")
    started = list(node_ids)
    members = list(membership) if membership is not None else started
    actors: Dict[str, Any] = {}
    for node_id in started:
        peers = [other for other in members if other != node_id]
        actors[node_id] = RaftActor.options(
            name=actor_name(node_id),
            namespace=namespace,
            get_if_exists=False,
        ).remote(
            node_id,
            peers,
            db_paths[node_id],
            state_factory,
            namespace,
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
