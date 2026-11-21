"""Background drivers that advance the Raft timers of every replica.

Timers live outside the replicas on purpose. An actor method that blocks on an
RPC must not stop the actor from serving incoming requests, and two actors
must never wait for each other. Each ticker is a detached Ray actor, so the
cluster keeps ticking even when every API process dies.
"""

import time
from typing import Any, Dict

from .raft_transport import DEFAULT_NAMESPACE

try:
    import ray
except ImportError:
    ray = None


DEFAULT_TICK_INTERVAL = 0.05
DEFAULT_TICKER_PREFIX = "sdcal-ticker-"


class _TickerReplica:
    """Plain implementation of the ticker loop."""

    def __init__(self, target: Any, interval: float) -> None:
        self.target = target
        self.interval = interval

    def run(self) -> None:
        while True:
            time.sleep(self.interval)
            try:
                # No timeout: the next tick is only issued once the previous
                # one finished, so a slow replica never queues pending ticks.
                ray.get(self.target.tick.remote())
            except Exception:
                # A dead or busy replica must not stop the ticker.
                continue


TickerActor = ray.remote(num_cpus=0)(_TickerReplica) if ray is not None else None


def start_tickers(
    actors: Dict[str, Any],
    interval: float = DEFAULT_TICK_INTERVAL,
    namespace: str = DEFAULT_NAMESPACE,
    prefix: str = DEFAULT_TICKER_PREFIX,
) -> Dict[str, Any]:
    """Start one detached ticker actor per replica."""
    if ray is None or TickerActor is None:
        raise RuntimeError("raft: ray is not installed")
    tickers: Dict[str, Any] = {}
    for node_id, actor in actors.items():
        ticker = TickerActor.options(
            name=f"{prefix}{node_id}",
            namespace=namespace,
            lifetime="detached",
            get_if_exists=False,
        ).remote(actor, interval)
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
