import pytest

from server.ray_app.raft_placement import (
    PLACEMENT_AUTO,
    PLACEMENT_OFF,
    PLACEMENT_STRICT,
    PlacementError,
    head_node_id,
    plan_node_placement,
)


def node_info(node_id: str, alive: bool = True, head: bool = False):
    resources = {"node:__internal_head__": 1.0} if head else {}
    return {
        "NodeID": node_id,
        "Alive": alive,
        "Resources": resources,
    }


def test_strict_placement_uses_one_distinct_node_per_replica():
    nodes = [
        node_info("head", head=True),
        node_info("worker-b"),
        node_info("worker-a"),
    ]

    assignments = plan_node_placement(
        ["n0", "n1", "n2"], PLACEMENT_STRICT, nodes
    )

    assert assignments == {
        "n0": "head",
        "n1": "worker-a",
        "n2": "worker-b",
    }
    assert len(set(assignments.values())) == 3


def test_head_node_is_identified_for_the_supervisor():
    nodes = [
        node_info("worker-a"),
        node_info("head", head=True),
    ]

    assert head_node_id(nodes) == "head"


def test_placement_prefers_workers_when_there_are_enough():
    nodes = [
        node_info("head", head=True),
        node_info("worker-b"),
        node_info("worker-a"),
    ]

    assignments = plan_node_placement(
        ["n0", "n1"], PLACEMENT_STRICT, nodes
    )

    assert set(assignments.values()) == {"worker-a", "worker-b"}
    assert "head" not in assignments.values()


def test_strict_placement_fails_without_enough_alive_nodes():
    nodes = [node_info("worker-a"), node_info("worker-b", alive=False)]

    with pytest.raises(PlacementError, match="requires 3 alive Ray nodes"):
        plan_node_placement(
            ["n0", "n1", "n2"], PLACEMENT_STRICT, nodes
        )


def test_auto_placement_falls_back_when_nodes_are_insufficient():
    nodes = [node_info("worker-a")]

    assert plan_node_placement(["n0", "n1"], PLACEMENT_AUTO, nodes) == {}


def test_off_placement_does_not_require_a_ray_cluster():
    assert plan_node_placement(["n0", "n1"], PLACEMENT_OFF) == {}


def test_unknown_placement_mode_is_rejected():
    with pytest.raises(ValueError, match="unknown placement mode"):
        plan_node_placement(["n0"], "elsewhere", [])
