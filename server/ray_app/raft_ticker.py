"""Background drivers that advance the Raft timers of every replica.

Timers live outside the replicas on purpose. An actor method that blocks on an
RPC must not stop the actor from serving incoming requests, and two actors
must never wait for each other. Each ticker is a detached Ray actor, so the
cluster keeps ticking even when every API process dies.

A ticker resolves its replica by name on every round, so it follows a replica
that the supervisor recreated after a crash.
"""

import time
from typing import Any, Dict, Iterable, List, Union

from .logging_config import get_logger
from .raft_transport import DEFAULT_NAMESPACE, actor_name

try:
    import ray
except ImportError:
    ray = None


DEFAULT_TICK_INTERVAL = 0.05
DEFAULT_TICKER_PREFIX = "sdcal-ticker-"


class _TickerReplica:
    """Plain implementation of the ticker loop."""

    def __init__(self, node_id: str, namespace: str, interval: float) -> None:
        self.node_id = node_id
        self.namespace = namespace
        self.interval = interval
        self.logger = get_logger("raft.ticker")

    def run(self) -> None:
        while True:
            time.sleep(self.interval)
            try:
                actor = ray.get_actor(
                    actor_name(self.node_id), namespace=self.namespace
                )
                # No timeout: the next tick is only issued once the previous
                # one finished, so a slow replica never queues pending ticks.
                ray.get(actor.tick.remote())
            except Exception as exc:
                # A dead or busy replica must not stop the ticker.
                self.logger.debug(
                    "tick skipped node_id=%s reason=%r", self.node_id, exc
                )
                continue


TickerActor = ray.remote(num_cpus=0)(_TickerReplica) if ray is not None else None


def start_tickers(
    replicas: Union[Dict[str, Any], Iterable[str]],
    interval: float = DEFAULT_TICK_INTERVAL,
    namespace: str = DEFAULT_NAMESPACE,
    prefix: str = DEFAULT_TICKER_PREFIX,
) -> Dict[str, Any]:
    """Start one detached ticker actor per replica."""
    if ray is None or TickerActor is None:
        raise RuntimeError("raft: ray is not installed")
    if isinstance(replicas, dict):
        node_ids: List[str] = list(replicas)
    else:
        node_ids = list(replicas)
    tickers: Dict[str, Any] = {}
    for node_id in node_ids:
        ticker = TickerActor.options(
            name=f"{prefix}{node_id}",
            namespace=namespace,
            lifetime="detached",
            get_if_exists=False,
        ).remote(node_id, namespace, interval)
        ticker.run.remote()
        tickers[node_id] = ticker
    return tickers


def stop_tickers(tickers: Dict[str, Any]) -> None:
    """Kill every ticker actor."""
    if ray is None:
        return
    for ticker in tickers.values():
        try:
            ray.kill(ticker)
        except Exception:
            pass
