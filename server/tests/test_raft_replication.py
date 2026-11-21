import random

import pytest

from server.ray_app.consensus.clock import FakeClock
from server.ray_app.consensus.node import NotLeaderError, RaftNode, Role
from server.ray_app.consensus.transport import InMemoryNetwork
from server.ray_app.fsm import CalendarFSM


class Recorder:
    def __init__(self):
        self.applied = []

    def apply(self, command):
        self.applied.append(command)
        return {"ok": True, "value": command}


def cluster(size: int = 5, seed: int = 7, apply_factory=None):
    clock = FakeClock()
    network = InMemoryNetwork()
    nodes = {}
    machines = {}
    factory = apply_factory or Recorder
    for index in range(size):
        node_id = f"n{index}"
        peers = [f"n{other}" for other in range(size) if other != index]
        machine = factory()
        node = RaftNode(
            node_id,
            peers,
            network,
            clock=clock,
            random_source=random.Random(seed + index),
            apply_command=machine.apply,
        )
        nodes[node_id] = node
        machines[node_id] = machine
        network.register(node_id, node.handle_message)
    return clock, network, nodes, machines


def elect_leader(clock, nodes, leader_id: str = "n0"):
    clock.advance(1.0)
    nodes[leader_id].tick()
    assert nodes[leader_id].role is Role.LEADER


def request_command(msg_id: str):
    return {
        "deployment_id": "dep1",
        "group_id": "team",
        "msg_id": msg_id,
        "type": "REQUEST",
        "uid": "alice:01J00000000000000000000001",
        "sender_id": "alice",
        "key_id": "key1",
        "sequence": 1,
        "seen_sequence": 0,
        "client_sent_at": "2026-11-21T00:00:00Z",
        "payload": {"title": "Diseño"},
    }


def test_propose_commits_and_applies_with_majority():
    clock, _, nodes, machines = cluster()
    elect_leader(clock, nodes)

    result = nodes["n0"].propose({"msg_id": "m1"})
    assert result == {"ok": True, "value": {"msg_id": "m1"}}
    assert nodes["n0"].commit_index == 1
    assert nodes["n0"].last_applied == 1
    assert machines["n0"].applied == [{"msg_id": "m1"}]

    nodes["n0"].send_heartbeats()
    for node_id, node in nodes.items():
        assert node.commit_index == 1
        assert node.last_applied == 1
        assert machines[node_id].applied == [{"msg_id": "m1"}]


def test_propose_without_quorum_does_not_commit():
    clock, network, nodes, machines = cluster()
    elect_leader(clock, nodes)
    for peer in ("n2", "n3", "n4"):
        network.cut("n0", peer)

    assert nodes["n0"].propose({"msg_id": "m1"}) is None
    assert nodes["n0"].commit_index == 0
    assert nodes["n0"].last_applied == 0
    assert machines["n0"].applied == []
    assert nodes["n0"].log.last_index == 1


def test_follower_catches_up_after_partition():
    clock, network, nodes, machines = cluster()
    elect_leader(clock, nodes)
    for peer in nodes:
        if peer != "n2":
            network.cut("n2", peer)

    assert nodes["n0"].propose({"msg_id": "m1"}) is not None
    assert nodes["n2"].last_applied == 0

    network.heal()
    nodes["n0"].send_heartbeats()
    assert nodes["n2"].commit_index == 1
    assert machines["n2"].applied == [{"msg_id": "m1"}]


def test_follower_cannot_propose():
    clock, _, nodes, _ = cluster()
    elect_leader(clock, nodes)
    with pytest.raises(NotLeaderError):
        nodes["n1"].propose({"msg_id": "m1"})


def test_commands_apply_in_order_on_every_replica():
    clock, _, nodes, machines = cluster()
    elect_leader(clock, nodes)
    for index in range(3):
        assert nodes["n0"].propose({"msg_id": f"m{index}"}) is not None

    nodes["n0"].send_heartbeats()
    expected = [{"msg_id": "m0"}, {"msg_id": "m1"}, {"msg_id": "m2"}]
    for machine in machines.values():
        assert machine.applied == expected


def test_new_leader_keeps_committed_entries():
    clock, network, nodes, machines = cluster()
    elect_leader(clock, nodes)
    assert nodes["n0"].propose({"msg_id": "m1"}) is not None
    nodes["n0"].send_heartbeats()

    network.unregister("n0")
    clock.advance(1.0)
    nodes["n2"].tick()
    assert nodes["n2"].role is Role.LEADER
    assert nodes["n2"].propose({"msg_id": "m2"}) is not None
    nodes["n2"].send_heartbeats()

    assert machines["n2"].applied == [{"msg_id": "m1"}, {"msg_id": "m2"}]
    assert machines["n3"].applied == [{"msg_id": "m1"}, {"msg_id": "m2"}]


def test_duplicate_command_is_deduplicated_by_fsm():
    clock, _, nodes, machines = cluster(apply_factory=CalendarFSM)
    elect_leader(clock, nodes)

    first = nodes["n0"].propose(request_command("01J00000000000000000000002"))
    second = nodes["n0"].propose(request_command("01J00000000000000000000002"))

    assert first["deduplicated"] is False
    assert second["deduplicated"] is True
    assert nodes["n0"].last_applied == 2
    assert machines["n0"].applied_index == 1


def test_status_includes_last_applied():
    clock, _, nodes, _ = cluster()
    elect_leader(clock, nodes)
    nodes["n0"].propose({"msg_id": "m1"})
    assert nodes["n0"].status()["last_applied"] == 1
