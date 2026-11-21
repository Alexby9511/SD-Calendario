"""Ray transport for Raft RPCs.

The consensus core only knows ``send(source, target, message)``. This module
maps that call onto named Ray actors so replicas can live in different
processes or nodes without changing the algorithm.
"""

from typing import Any

from .consensus.transport import TransportError

try:
    import ray
except ImportError:
    ray = None


DEFAULT_NAMESPACE = "sdcal"
DEFAULT_ACTOR_PREFIX = "sdcal-raft-"


def actor_name(node_id: str, prefix: str = DEFAULT_ACTOR_PREFIX) -> str:
    """Return the deterministic Ray actor name for a replica."""
    return f"{prefix}{node_id}"


class RayTransport:
    """Synchronous Raft transport backed by named Ray actors."""

    def __init__(
        self,
        namespace: str = DEFAULT_NAMESPACE,
        actor_prefix: str = DEFAULT_ACTOR_PREFIX,
        timeout: float = 0.5,
    ) -> None:
        if ray is None:
            raise RuntimeError("raft: ray is not installed")
        self.namespace = namespace
        self.actor_prefix = actor_prefix
        self.timeout = timeout

    def send(self, source: str, target: str, message: Any) -> Any:
        """Deliver ``message`` to the target actor and wait for its reply."""
        try:
            actor = ray.get_actor(
                actor_name(target, self.actor_prefix),
                namespace=self.namespace,
            )
            return ray.get(
                actor.handle_message.remote(message),
                timeout=self.timeout,
            )
        except Exception as exc:
            raise TransportError(
                f"raft: message from {source} to {target} dropped: {exc!r}"
            ) from exc
