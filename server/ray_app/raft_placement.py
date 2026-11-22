"""Placement planning for Raft replicas on distinct Ray nodes.

Each replica keeps its SQLite database on the node where its actor runs. A
strict node affinity prevents Ray from silently moving that actor to another
node, where the local database would not exist.
"""

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

PLACEMENT_AUTO = "auto"
PLACEMENT_OFF = "off"
PLACEMENT_STRICT = "strict"
PLACEMENT_MODES = (PLACEMENT_AUTO, PLACEMENT_OFF, PLACEMENT_STRICT)


class PlacementError(RuntimeError):
    """Raised when the requested Raft placement cannot be satisfied."""


def plan_node_placement(
    replica_ids: Iterable[str],
    mode: str = PLACEMENT_AUTO,
    node_info: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, str]:
    """Assign each replica to one distinct alive Ray node.

    Worker nodes are preferred over the head node. ``strict`` fails when the
    cluster does not have enough alive nodes. ``auto`` uses strict placement
    when possible and otherwise returns an empty mapping for local development.
    """
    if mode not in PLACEMENT_MODES:
        raise ValueError(f"raft: unknown placement mode {mode!r}")

    replicas = list(replica_ids)
    if mode == PLACEMENT_OFF or not replicas:
        return {}

    nodes = _alive_nodes(node_info)
    if mode == PLACEMENT_AUTO and len(nodes) < len(replicas):
        return {}

    if len(nodes) < len(replicas):
        raise PlacementError(
            "raft: strict placement requires "
            f"{len(replicas)} alive Ray nodes, found {len(nodes)}"
        )

    selected = _select_nodes(nodes, len(replicas))
    return dict(zip(replicas, selected))


def head_node_id(
    node_info: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Optional[str]:
    """Return the alive Ray head node ID when it can be identified."""
    for info in _alive_nodes(node_info):
        if _is_head(info):
            return str(info["NodeID"])
    return None


def strict_node_affinity(node_id: str) -> Any:
    """Return a Ray strategy that pins an actor to ``node_id``."""
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    return NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)


def _alive_nodes(
    node_info: Optional[Sequence[Mapping[str, Any]]],
) -> List[Mapping[str, Any]]:
    if node_info is None:
        try:
            import ray
        except ImportError as exc:
            raise PlacementError("raft: ray is not installed") from exc
        if not ray.is_initialized():
            raise PlacementError("raft: Ray is not initialized")
        node_info = ray.nodes()

    alive: Dict[str, Mapping[str, Any]] = {}
    for info in node_info:
        node_id = info.get("NodeID")
        if not node_id:
            continue
        if not info.get("Alive", info.get("alive", False)):
            continue
        alive[str(node_id)] = info
    return [alive[node_id] for node_id in sorted(alive)]


def _select_nodes(
    nodes: Sequence[Mapping[str, Any]], count: int
) -> List[str]:
    workers = [info for info in nodes if not _is_head(info)]
    if len(workers) >= count:
        selected = workers[:count]
    else:
        selected = list(nodes[:count])
    return [str(info["NodeID"]) for info in selected]


def _is_head(info: Mapping[str, Any]) -> bool:
    resources = info.get("Resources", {})
    return isinstance(resources, Mapping) and "node:__internal_head__" in resources
