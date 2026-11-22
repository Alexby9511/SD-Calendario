"""Ray transport for Raft RPCs.

The consensus core only knows ``send(source, target, message)``. This module
maps that call onto named Ray actors so replicas can live in different
processes or nodes without changing the algorithm.
"""

import time
from typing import Any, Dict

from .consensus.transport import TransportError

try:
    import ray
except ImportError:
    ray = None


DEFAULT_NAMESPACE = "sdcal"
DEFAULT_ACTOR_PREFIX = "sdcal-raft-"
DEFAULT_TIMEOUT = 0.1
DEFAULT_FAILURE_THRESHOLD = 2
DEFAULT_FAILURE_COOLDOWN = 0.5


def actor_name(node_id: str, prefix: str = DEFAULT_ACTOR_PREFIX) -> str:
    """Return the deterministic Ray actor name for a replica."""
    return f"{prefix}{node_id}"


class RayTransport:
    """Synchronous Raft transport backed by named Ray actors.

    A peer that keeps failing is skipped for a short cooldown. Without it a
    single dead replica would delay every heartbeat round until the RPC
    timeout expired, which is longer than the election timeout and would
    trigger spurious elections.
    """

    def __init__(
        self,
        namespace: str = DEFAULT_NAMESPACE,
        actor_prefix: str = DEFAULT_ACTOR_PREFIX,
        timeout: float = DEFAULT_TIMEOUT,
        failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
        failure_cooldown: float = DEFAULT_FAILURE_COOLDOWN,
    ) -> None:
        if ray is None:
            raise RuntimeError("raft: ray is not installed")
        self.namespace = namespace
        self.actor_prefix = actor_prefix
        self.timeout = timeout
        self.failure_threshold = failure_threshold
        self.failure_cooldown = failure_cooldown
        self._failures: Dict[str, int] = {}
        self._blocked_until: Dict[str, float] = {}
        self._actor_ids: Dict[str, str] = {}

    def send(self, source: str, target: str, message: Any) -> Any:
        """Deliver ``message`` to the target actor and wait for its reply."""
        now = time.monotonic()
        try:
            actor = ray.get_actor(
                actor_name(target, self.actor_prefix),
                namespace=self.namespace,
            )
        except Exception as exc:
            self._register_failure(target, now)
            raise TransportError(
                f"raft: message from {source} to {target} dropped: {exc!r}"
            ) from exc

        actor_id = self._actor_identity(actor)
        if self._blocked_until.get(target, 0.0) > now:
            previous_actor_id = self._actor_ids.get(target)
            if previous_actor_id is None or actor_id != previous_actor_id:
                # The target was recreated after being marked as down. Its new
                # identity proves that it is safe to retry immediately.
                self._failures.pop(target, None)
                self._blocked_until.pop(target, None)
            else:
                raise TransportError(f"raft: target {target} is marked as down")

        try:
            reply = ray.get(
                actor.handle_message.remote(message),
                timeout=self.timeout,
            )
        except Exception as exc:
            self._actor_ids[target] = actor_id
            self._register_failure(target, now)
            raise TransportError(
                f"raft: message from {source} to {target} dropped: {exc!r}"
            ) from exc
        self._failures.pop(target, None)
        self._blocked_until.pop(target, None)
        self._actor_ids[target] = actor_id
        return reply

    @staticmethod
    def _actor_identity(actor: Any) -> str:
        actor_id = getattr(actor, "_actor_id", None)
        if actor_id is not None:
            return str(actor_id)
        return repr(actor)

    def _register_failure(self, target: str, now: float) -> None:
        failures = self._failures.get(target, 0) + 1
        if failures >= self.failure_threshold:
            self._blocked_until[target] = now + self.failure_cooldown
            self._failures[target] = 0
        else:
            self._failures[target] = failures

    def reset(self, target: str) -> None:
        """Forget the failures recorded for ``target``."""
        self._failures.pop(target, None)
        self._blocked_until.pop(target, None)
        self._actor_ids.pop(target, None)
