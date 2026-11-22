"""Recreates dead Raft replicas so the cluster heals itself.

The supervisor is a detached Ray actor: it keeps running when an API process
dies. A recreated replica reloads its term, vote, log and snapshot from its
own SQLite database and rejoins the cluster as a follower. If strict node
placement is configured, recreation keeps the actor on the same Ray node and
waits when that node is unavailable.
"""

import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional

from .logging_config import get_logger
from .raft_actor import (
    DEFAULT_ELECTION_TIMEOUT,
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_TRANSPORT_TIMEOUT,
    RaftActor,
)
from .raft_placement import placement_options, placement_target_alive
from .raft_startup import create_named_actor
from .raft_ticker import (
    DEFAULT_TICK_INTERVAL,
    DEFAULT_TICKER_PREFIX,
    TickerActor,
)
from .raft_transport import DEFAULT_NAMESPACE, actor_name
from .state import CalendarState

try:
    import ray
except ImportError:
    ray = None


DEFAULT_SUPERVISOR_NAME = "sdcal-supervisor"
DEFAULT_CHECK_INTERVAL = 0.5
DEFAULT_CHECK_TIMEOUT = 2.0


class _SupervisorReplica:
    """Plain implementation of the supervision loop."""

    def __init__(
        self,
        node_ids: List[str],
        db_paths: Dict[str, str],
        namespace: str,
        state_factory: Callable[[], Any],
        interval: float,
        timeout: float,
        node_assignments: Dict[str, str],
        ticker_interval: float,
        ticker_prefix: str,
    ) -> None:
        self.node_ids = list(node_ids)
        self.db_paths = dict(db_paths)
        self.namespace = namespace
        self.state_factory = state_factory
        self.interval = interval
        self.timeout = timeout
        self.node_assignments = dict(node_assignments)
        self.ticker_interval = ticker_interval
        self.ticker_prefix = ticker_prefix
        self._known_placements: Dict[str, str] = dict(node_assignments)
        self._started = False
        self._thread: Optional[threading.Thread] = None
        self.logger = get_logger("raft.supervisor")

    def node_id(self) -> str:
        """Return the physical Ray node where the supervisor runs."""
        return ray.get_runtime_context().get_node_id()

    def run(self) -> None:
        """Start the supervision loop in a daemon thread and return.

        A blocking actor method would make the supervisor unreachable for
        inspection and would prevent a second caller from attaching to it.
        """
        if self._started:
            return
        self._started = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while True:
            try:
                self.check()
            except Exception as exc:
                self.logger.warning("supervision cycle failed reason=%r", exc)
            time.sleep(self.interval)

    def check(self) -> List[str]:
        """Recreate every replica or ticker that is not answering."""
        recovered: List[str] = []
        for node_id in self.node_ids:
            status = self._status(node_id)
            if status is None:
                if self._recreate(node_id):
                    recovered.append(node_id)
            else:
                self._learn_placement(node_id, status)
            self._ensure_ticker(node_id)
        return recovered

    def _status(self, node_id: str) -> Optional[Dict[str, Any]]:
        try:
            actor = ray.get_actor(
                actor_name(node_id), namespace=self.namespace
            )
        except Exception:
            return None
        try:
            return ray.get(actor.status.remote(), timeout=self.timeout)
        except Exception:
            return None

    def _learn_placement(self, node_id: str, status: Dict[str, Any]) -> None:
        """Remember the placement target reported by a live replica."""
        placement = status.get("placement_key")
        if placement:
            self._known_placements[node_id] = str(placement)

    def _is_alive(self, node_id: str) -> bool:
        return self._status(node_id) is not None

    def _node_is_alive(self, node_id: str) -> bool:
        try:
            return placement_target_alive(node_id)
        except Exception:
            return False

    def _recreate(self, node_id: str) -> bool:
        peers = [other for other in self.node_ids if other != node_id]
        target_node = self._known_placements.get(node_id)
        if target_node and not self._node_is_alive(target_node):
            self.logger.debug(
                "replica waits for its Ray node node_id=%s target_node_id=%s",
                node_id,
                target_node,
            )
            return False

        options: Dict[str, Any] = {
            "name": actor_name(node_id),
            "namespace": self.namespace,
            "lifetime": "detached",
            "get_if_exists": False,
        }
        if target_node:
            options.update(placement_options(target_node))
        try:
            RaftActor.options(**options).remote(
                node_id,
                peers,
                self.db_paths[node_id],
                self.state_factory,
                self.namespace,
                DEFAULT_TRANSPORT_TIMEOUT,
                DEFAULT_ELECTION_TIMEOUT,
                DEFAULT_HEARTBEAT_INTERVAL,
                target_node,
            )
        except Exception as exc:
            self.logger.warning(
                "could not recreate replica node_id=%s reason=%r", node_id, exc
            )
            return False
        self.logger.info("recreated replica node_id=%s", node_id)
        return True

    def _ticker_is_alive(self, node_id: str) -> bool:
        try:
            ticker = ray.get_actor(
                f"{self.ticker_prefix}{node_id}", namespace=self.namespace
            )
        except Exception:
            return False
        try:
            ray.get(ticker.node_id.remote(), timeout=self.timeout)
        except Exception:
            return False
        return True

    def _ensure_ticker(self, node_id: str) -> bool:
        """Recreate a missing ticker so the replica keeps its timers."""
        if self._ticker_is_alive(node_id):
            return False
        target_node = self._known_placements.get(node_id)
        if target_node and not self._node_is_alive(target_node):
            return False

        options: Dict[str, Any] = {"lifetime": "detached"}
        if target_node:
            options.update(placement_options(target_node))
        try:
            create_named_actor(
                TickerActor,
                node_id,
                self.namespace,
                self.ticker_interval,
                name=f"{self.ticker_prefix}{node_id}",
                namespace=self.namespace,
                start_method="run",
                **options,
            )
        except Exception as exc:
            self.logger.warning(
                "could not recreate ticker node_id=%s reason=%r", node_id, exc
            )
            return False
        self.logger.info("recreated ticker node_id=%s", node_id)
        return True


SupervisorActor = (
    ray.remote(num_cpus=0)(_SupervisorReplica) if ray is not None else None
)


def start_supervisor(
    node_ids: Iterable[str],
    db_paths: Dict[str, str],
    namespace: str = DEFAULT_NAMESPACE,
    state_factory: Callable[[], Any] = CalendarState,
    interval: float = DEFAULT_CHECK_INTERVAL,
    timeout: float = DEFAULT_CHECK_TIMEOUT,
    name: str = DEFAULT_SUPERVISOR_NAME,
    node_assignments: Optional[Dict[str, str]] = None,
    supervisor_node_id: Optional[str] = None,
    ticker_interval: float = DEFAULT_TICK_INTERVAL,
    ticker_prefix: str = DEFAULT_TICKER_PREFIX,
) -> Any:
    """Start (or attach to) the detached supervisor of the replicas."""
    if ray is None or SupervisorActor is None:
        raise RuntimeError("raft: ray is not installed")
    options: Dict[str, Any] = {"lifetime": "detached"}
    if supervisor_node_id:
        options.update(placement_options(supervisor_node_id))
    return create_named_actor(
        SupervisorActor,
        list(node_ids),
        dict(db_paths),
        namespace,
        state_factory,
        interval,
        timeout,
        dict(node_assignments or {}),
        ticker_interval,
        ticker_prefix,
        name=name,
        namespace=namespace,
        start_method="run",
        **options,
    )


def stop_supervisor(supervisor: Any) -> None:
    """Kill the supervisor actor."""
    if ray is None:
        return
    try:
        ray.kill(supervisor)
    except Exception:
        pass
