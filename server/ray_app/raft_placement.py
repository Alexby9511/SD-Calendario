"""Placement planning for Raft replicas on distinct Ray nodes.

Each replica keeps its SQLite database on the node where its actor runs, so
the placement must survive the restart of that node. A Ray ``NodeID`` changes
when a node restarts, therefore a node that declares a unique custom resource
(for example ``sdcal-node-1``) is identified by that resource instead. Nodes
without such a resource fall back to their ``NodeID``, which is enough for
long-lived clusters but not for a node that rejoins the cluster.
"""

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

PLACEMENT_AUTO = "auto"
PLACEMENT_OFF = "off"
PLACEMENT_STRICT = "strict"
PLACEMENT_MODES = (PLACEMENT_AUTO, PLACEMENT_OFF, PLACEMENT_STRICT)

RESOURCE_PREFIX = "sdcal-node-"
NODE_KIND = "node"
RESOURCE_KIND = "resource"


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

    The returned keys are placement targets (``"node:<id>"`` or
    ``"resource:<name>"``); see :func:`placement_options`.
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
    assignments = {
        replica_id: _node_placement_key(info, nodes)
        for replica_id, info in zip(replicas, selected)
    }
    targets = list(assignments.values())
    if len(set(targets)) != len(targets):
        raise PlacementError(
            "raft: placement requires one distinct target per replica"
        )
    return assignments


def placement_key(kind: str, value: str) -> str:
    """Build a placement key from its kind and value."""
    return f"{kind}:{value}"


def parse_placement_key(key: str) -> Tuple[str, str]:
    """Split a placement key into its kind and value."""
    kind, separator, value = key.partition(":")
    if not separator or not value:
        raise ValueError(f"raft: invalid placement key {key!r}")
    if kind not in (NODE_KIND, RESOURCE_KIND):
        raise ValueError(f"raft: unknown placement kind {kind!r}")
    return kind, value


def placement_options(key: str) -> Dict[str, Any]:
    """Return the Ray actor options that pin an actor to a placement target."""
    kind, value = parse_placement_key(key)
    if kind == RESOURCE_KIND:
        # A unique custom resource exists only on the intended node, so
        # requesting a fraction of it pins the actor to that node and keeps
        # the pin valid when the node rejoins with a new Ray NodeID.
        return {"resources": {value: 0.001}}
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    return {
        "scheduling_strategy": NodeAffinitySchedulingStrategy(
            node_id=value, soft=False
        )
    }


def placement_target_alive(
    key: str,
    node_info: Optional[Sequence[Mapping[str, Any]]] = None,
) -> bool:
    """Return whether the node behind a placement target is alive."""
    kind, value = parse_placement_key(key)
    nodes = _alive_nodes(node_info)
    if kind == NODE_KIND:
        return any(str(info.get("NodeID")) == value for info in nodes)
    return any(_has_resource(info, value) for info in nodes)


def head_placement_key(
    node_info: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Optional[str]:
    """Return the placement target of the head node, when it can be found."""
    nodes = _alive_nodes(node_info)
    for info in nodes:
        if _is_head(info):
            return _node_placement_key(info, nodes)
    return None


def head_node_id(
    node_info: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Optional[str]:
    """Return the alive Ray head node ID when it can be identified."""
    for info in _alive_nodes(node_info):
        if _is_head(info):
            return str(info["NodeID"])
    return None


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
) -> List[Mapping[str, Any]]:
    workers = [info for info in nodes if not _is_head(info)]
    if len(workers) >= count:
        selected = workers[:count]
    else:
        selected = list(nodes[:count])
    return selected


def _node_placement_key(
    info: Mapping[str, Any],
    all_nodes: Sequence[Mapping[str, Any]],
) -> str:
    """Return the stable placement key of a node."""
    for name in _stable_resources(info, all_nodes):
        return placement_key(RESOURCE_KIND, name)
    return placement_key(NODE_KIND, str(info["NodeID"]))


def _stable_resources(
    info: Mapping[str, Any],
    all_nodes: Sequence[Mapping[str, Any]],
) -> List[str]:
    """Return the custom resources that identify this node uniquely."""
    names = []
    for name in _resource_names(info):
        if name in names:
            continue
        owners = [
            other
            for other in all_nodes
            if _has_resource(other, name)
        ]
        if len(owners) == 1:
            names.append(name)
    return names


def _resource_names(info: Mapping[str, Any]) -> List[str]:
    resources = info.get("Resources", {})
    if not isinstance(resources, Mapping):
        return []
    return sorted(
        name
        for name, quantity in resources.items()
        if isinstance(name, str)
        and name.startswith(RESOURCE_PREFIX)
        and isinstance(quantity, (int, float))
        and quantity >= 1
    )


def _has_resource(info: Mapping[str, Any], name: str) -> bool:
    resources = info.get("Resources", {})
    if not isinstance(resources, Mapping):
        return False
    quantity = resources.get(name)
    return isinstance(quantity, (int, float)) and quantity >= 1


def _is_head(info: Mapping[str, Any]) -> bool:
    resources = info.get("Resources", {})
    return isinstance(resources, Mapping) and "node:__internal_head__" in resources
