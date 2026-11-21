"""Recreates dead Raft replicas so the cluster heals itself.

The supervisor is a detached Ray actor: it keeps running when an API process
dies. A recreated replica reloads its term, vote, log and snapshot from its
own SQLite database and rejoins the cluster as a follower.
"""

import time
from typing import Any, Callable, Dict, Iterable, List, Optional

from .logging_config import get_logger
from .raft_actor import RaftActor
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
    ) -> None:
        self.node_ids = list(node_ids)
        self.db_paths = dict(db_paths)
        self.namespace = namespace
        self.state_factory = state_factory
        self.interval = interval
        self.timeout = timeout
        self._started = False
        self.logger = get_logger("raft.supervisor")

    def run(self) -> None:
        if self._started:
            return
        self._started = True
        while True:
            try:
                self.check()
            except Exception as exc:
                self.logger.warning("supervision cycle failed reason=%r", exc)
            time.sleep(self.interval)

    def check(self) -> List[str]:
        """Recreate every replica that is not answering."""
        recovered: List[str] = []
        for node_id in self.node_ids:
            if self._is_alive(node_id):
                continue
            if self._recreate(node_id):
                recovered.append(node_id)
        return recovered

    def _is_alive(self, node_id: str) -> bool:
        try:
            actor = ray.get_actor(
                actor_name(node_id), namespace=self.namespace
            )
        except Exception:
            return False
        try:
            ray.get(actor.status.remote(), timeout=self.timeout)
        except Exception:
            return False
        return True

    def _recreate(self, node_id: str) -> bool:
        peers = [other for other in self.node_ids if other != node_id]
        try:
            RaftActor.options(
                name=actor_name(node_id),
                namespace=self.namespace,
                lifetime="detached",
                get_if_exists=False,
            ).remote(
                node_id,
                peers,
                self.db_paths[node_id],
                self.state_factory,
                self.namespace,
            )
        except Exception as exc:
            self.logger.warning(
                "could not recreate replica node_id=%s reason=%r", node_id, exc
            )
            return False
        self.logger.info("recreated replica node_id=%s", node_id)
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
) -> Any:
    """Start (or attach to) the detached supervisor of the replicas."""
    if ray is None or SupervisorActor is None:
        raise RuntimeError("raft: ray is not installed")
    supervisor = SupervisorActor.options(
        name=name,
        namespace=namespace,
        lifetime="detached",
        get_if_exists=True,
    ).remote(
        list(node_ids),
        dict(db_paths),
        namespace,
        state_factory,
        interval,
        timeout,
    )
    supervisor.run.remote()
    return supervisor


def stop_supervisor(supervisor: Any) -> None:
    """Kill the supervisor actor."""
    if ray is None:
        return
    try:
        ray.kill(supervisor)
    except Exception:
        pass
