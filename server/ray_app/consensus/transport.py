"""Transport abstraction for Raft RPCs.

The in-memory network keeps the consensus core independent from Ray and lets
tests drop, delay or partition messages deterministically.
"""

from typing import Any, Callable, Dict, Iterable, Optional, Set, Tuple


class TransportError(Exception):
    """Raised when a message cannot be delivered."""


class InMemoryNetwork:
    """Synchronous message bus with controllable cuts between nodes."""

    def __init__(self) -> None:
        self._handlers: Dict[str, Callable[[Any], Any]] = {}
        self._cuts: Set[Tuple[str, str]] = set()

    def register(self, node_id: str, handler: Callable[[Any], Any]) -> None:
        if node_id in self._handlers:
            raise ValueError(f"raft: node {node_id!r} already registered")
        self._handlers[node_id] = handler

    def unregister(self, node_id: str) -> None:
        self._handlers.pop(node_id, None)

    def nodes(self) -> Iterable[str]:
        return tuple(self._handlers)

    def send(self, source: str, target: str, message: Any) -> Any:
        """Deliver ``message`` from ``source`` to ``target`` and return its reply."""
        if self._blocked(source, target):
            raise TransportError(f"raft: message from {source} to {target} dropped")
        handler = self._handlers.get(target)
        if handler is None:
            raise TransportError(f"raft: unknown node {target!r}")
        return handler(message)

    def cut(self, a: str, b: str) -> None:
        """Drop every message between ``a`` and ``b`` in both directions."""
        self._cuts.add((a, b))
        self._cuts.add((b, a))

    def heal(self, a: Optional[str] = None, b: Optional[str] = None) -> None:
        """Remove one cut, or every cut when no nodes are given."""
        if a is None or b is None:
            self._cuts.clear()
            return
        self._cuts.discard((a, b))
        self._cuts.discard((b, a))

    def _blocked(self, source: str, target: str) -> bool:
        return (source, target) in self._cuts
