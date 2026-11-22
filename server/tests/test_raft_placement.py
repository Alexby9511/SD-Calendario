import pytest

from server.ray_app.raft_placement import (
    PLACEMENT_AUTO,
    PLACEMENT_OFF,
    PLACEMENT_STRICT,
    PlacementError,
    head_node_id,
    head_placement_key,
    parse_placement_key,
    placement_options,
    placement_target_alive,
    plan_node_placement,
)


def node_info(
    node_id: str,
    alive: bool = True,
    head: bool = False,
    resources=None,
):
    node_resources = dict(resources or {})
    if head:
        node_resources["node:__internal_head__"] = 1.0
    return {
        "NodeID": node_id,
        "Alive": alive,
        "Resources": node_resources,
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
        "n0": "node:head",
        "n1": "node:worker-a",
        "n2": "node:worker-b",
    }
    assert len(set(assignments.values())) == 3


def test_placement_prefers_workers_when_there_are_enough():
    nodes = [
        node_info("head", head=True),
        node_info("worker-b"),
        node_info("worker-a"),
    ]

    assignments = plan_node_placement(
        ["n0", "n1"], PLACEMENT_STRICT, nodes
    )

    assert set(assignments.values()) == {"node:worker-a", "node:worker-b"}
    assert "node:head" not in assignments.values()


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


def test_node_with_a_unique_resource_is_placed_by_resource():
    nodes = [
        node_info("worker-a", resources={"sdcal-node-1": 1.0}),
        node_info("worker-b", resources={"sdcal-node-2": 1.0}),
    ]

    assignments = plan_node_placement(["n0", "n1"], PLACEMENT_STRICT, nodes)

    assert assignments == {
        "n0": "resource:sdcal-node-1",
        "n1": "resource:sdcal-node-2",
    }
    assert placement_options("resource:sdcal-node-1") == {
        "resources": {"sdcal-node-1": 0.001}
    }


def test_shared_resource_falls_back_to_the_ray_node_id():
    nodes = [
        node_info("worker-a", resources={"sdcal-node-1": 1.0}),
        node_info("worker-b", resources={"sdcal-node-1": 1.0}),
    ]

    assignments = plan_node_placement(["n0", "n1"], PLACEMENT_STRICT, nodes)

    assert assignments == {
        "n0": "node:worker-a",
        "n1": "node:worker-b",
    }


def test_placement_target_alive_tracks_resources_and_node_ids():
    nodes = [
        node_info("worker-a", resources={"sdcal-node-1": 1.0}),
        node_info("worker-b", alive=False),
    ]

    assert placement_target_alive("resource:sdcal-node-1", nodes) is True
    assert placement_target_alive("node:worker-b", nodes) is False
    assert placement_target_alive("node:worker-a", nodes) is True


def test_head_placement_key_prefers_the_head_resource():
    nodes = [
        node_info("worker-a", resources={"sdcal-node-1": 1.0}),
        node_info("head", head=True, resources={"sdcal-node-head": 1.0}),
    ]

    assert head_placement_key(nodes) == "resource:sdcal-node-head"
    assert head_node_id(nodes) == "head"


def test_placement_keys_are_parsed_and_validated():
    assert parse_placement_key("node:abc") == ("node", "abc")
    assert parse_placement_key("resource:sdcal-node-1") == (
        "resource",
        "sdcal-node-1",
    )
    with pytest.raises(ValueError, match="invalid placement key"):
        parse_placement_key("abc")
    with pytest.raises(ValueError, match="unknown placement kind"):
        parse_placement_key("cluster:abc")
