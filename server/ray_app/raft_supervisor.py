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
DEFAULT_UNHEALTHY_THRESHOLD = 3
_UNRESPONSIVE_STATUS = "_supervisor_unresponsive"


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
        strict_placement: bool = False,
        unhealthy_threshold: int = DEFAULT_UNHEALTHY_THRESHOLD,
    ) -> None:
        if (
            not isinstance(unhealthy_threshold, int)
            or isinstance(unhealthy_threshold, bool)
            or unhealthy_threshold < 1
        ):
            raise ValueError("raft: unhealthy threshold must be a positive integer")
        self.node_ids = list(node_ids)
        self.db_paths = dict(db_paths)
        self.namespace = namespace
        self.state_factory = state_factory
        self.interval = interval
        self.timeout = timeout
        self.node_assignments = dict(node_assignments)
        self.ticker_interval = ticker_interval
        self.ticker_prefix = ticker_prefix
        self.strict_placement = strict_placement
        self._known_placements: Dict[str, str] = dict(node_assignments)
        missing_placements = set(self.node_ids) - set(self._known_placements)
        if self.strict_placement and missing_placements:
            raise ValueError(
                "raft: strict supervisor requires placements for "
                + ", ".join(sorted(missing_placements))
            )
        self.unhealthy_threshold = unhealthy_threshold
        self._unhealthy_checks: Dict[str, int] = {}
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
            elif status.get(_UNRESPONSIVE_STATUS):
                pass
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
            self._unhealthy_checks.pop(node_id, None)
            return None
        try:
            status = ray.get(actor.status.remote(), timeout=self.timeout)
        except Exception as exc:
            failures = self._unhealthy_checks.get(node_id, 0) + 1
            self._unhealthy_checks[node_id] = failures
            if failures < self.unhealthy_threshold:
                self.logger.warning(
                    "replica health check timed out node_id=%s failures=%d/%d",
                    node_id,
                    failures,
                    self.unhealthy_threshold,
                )
                return {_UNRESPONSIVE_STATUS: True}
            self.logger.error(
                "terminating unresponsive replica node_id=%s failures=%d "
                "reason=%r",
                node_id,
                failures,
                exc,
            )
            if self._terminate_unresponsive(node_id, actor):
                self._unhealthy_checks.pop(node_id, None)
                return None
            return {_UNRESPONSIVE_STATUS: True}
        self._unhealthy_checks.pop(node_id, None)
        return status

    def _learn_placement(self, node_id: str, status: Dict[str, Any]) -> None:
        """Remember the placement target reported by a live replica."""
        placement = status.get("placement_key")
        if not placement and status.get("ray_node_id"):
            placement = f"node:{status['ray_node_id']}"
        if placement:
            self._known_placements[node_id] = str(placement)

    def placements(self) -> Dict[str, str]:
        """Return the known replica targets for startup recovery."""
        return dict(self._known_placements)

    def _terminate_unresponsive(self, node_id: str, actor: Any) -> bool:
        try:
            ray.kill(actor, no_restart=True)
        except Exception as exc:
            self.logger.warning(
                "could not terminate unresponsive replica node_id=%s reason=%r",
                node_id,
                exc,
            )
        deadline = time.monotonic() + max(self.timeout, 0.5)
        while time.monotonic() < deadline:
            try:
                ray.get_actor(actor_name(node_id), namespace=self.namespace)
            except Exception:
                return True
            time.sleep(0.05)
        self.logger.warning(
            "unresponsive replica still registered node_id=%s", node_id
        )
        return False

    def _is_alive(self, node_id: str) -> bool:
        return self._status(node_id) is not None

    def _node_is_alive(self, node_id: str) -> bool:
        try:
            return placement_target_alive(node_id)
        except Exception:
            return False

    def _recreate(self, node_id: str) -> bool:
        if self.strict_placement and not self._known_placements.get(node_id):
            self.logger.warning(
                "strict placement refused recreation without known placement "
                "node_id=%s",
                node_id,
            )
            return False
        peers = [other for other in self.node_ids if other != node_id]
        target_node = self._known_placements.get(node_id)
        if target_node and not self._node_is_alive(target_node):
            self.logger.debug(
                "replica waits for its Ray node node_id=%s target_node_id=%s",
                node_id,
                target_node,
            )
            return False

        options: Dict[str, Any] = {"lifetime": "detached", "get_if_exists": True}
        if target_node:
            options.update(placement_options(target_node))
        try:
            create_named_actor(
                RaftActor,
                node_id,
                peers,
                self.db_paths[node_id],
                self.state_factory,
                self.namespace,
                DEFAULT_TRANSPORT_TIMEOUT,
                DEFAULT_ELECTION_TIMEOUT,
                DEFAULT_HEARTBEAT_INTERVAL,
                target_node,
                name=actor_name(node_id),
                namespace=self.namespace,
                start_method="status",
                wait_for_start=True,
                attach_timeout=self.timeout,
                **options,
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
                wait_for_start=True,
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
    strict_placement: bool = False,
    unhealthy_threshold: int = DEFAULT_UNHEALTHY_THRESHOLD,
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
        strict_placement,
        unhealthy_threshold,
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
