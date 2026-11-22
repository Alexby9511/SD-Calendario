"""Level-2 chaos scenarios at the consensus core.

The service must survive two independent failures without losing confirmed
commands. These tests isolate the old leader, elect a new one with the
remaining quorum, and check that the isolated leader can neither commit nor
create a split brain when it rejoins.
"""

import logging
import random
import re

from server.ray_app.consensus.clock import FakeClock
from server.ray_app.consensus.node import RaftNode, Role
from server.ray_app.consensus.transport import InMemoryNetwork
from server.ray_app.fsm import CalendarFSM
from server.ray_app.logging_config import _formatter


def cluster(size: int = 5):
    clock = FakeClock()
    network = InMemoryNetwork()
    nodes = {}
    machines = {}
    for index in range(size):
        node_id = f"n{index}"
        peers = [f"n{other}" for other in range(size) if other != index]
        machine = CalendarFSM()
        node = RaftNode(
            node_id,
            peers,
            network,
            clock=clock,
            election_timeout=(0.2, 0.2),
            heartbeat_interval=0.05,
            random_source=random.Random(index),
            apply_command=machine.apply,
        )
        nodes[node_id] = node
        machines[node_id] = machine
        network.register(node_id, node.handle_message)
    return clock, network, nodes, machines


def request_command(msg_id: str, title: str = "Diseño"):
    return {
        "deployment_id": "dep1",
        "group_id": "team",
        "msg_id": msg_id,
        "type": "REQUEST",
        "uid": f"alice:{msg_id}",
        "sender_id": "alice",
        "key_id": "key1",
        "sequence": 1,
        "seen_sequence": 0,
        "client_sent_at": "2026-11-21T00:00:00Z",
        "payload": {"title": title},
    }


def elect_leader(clock, nodes, leader_id: str = "n0"):
    clock.advance(1.0)
    nodes[leader_id].tick()
    assert nodes[leader_id].role is Role.LEADER


def isolate(network, node_id, peers):
    for peer in peers:
        if peer != node_id:
            network.cut(node_id, peer)


def test_isolated_old_leader_cannot_commit_and_rejoins_as_follower():
    clock, network, nodes, machines = cluster()
    elect_leader(clock, nodes)
    assert nodes["n0"].propose(request_command("m1"))["code"] == 200
    nodes["n0"].send_heartbeats()
    assert all(machine.applied_index == 1 for machine in machines.values())

    isolate(network, "n0", nodes)
    assert nodes["n0"].propose(request_command("m2")) is None
    assert nodes["n0"].commit_index == 1
    assert machines["n0"].applied_index == 1

    clock.advance(1.0)
    nodes["n2"].tick()
    assert nodes["n2"].role is Role.LEADER
    assert nodes["n2"].current_term == 2
    assert nodes["n2"].propose(request_command("m3"))["code"] == 200
    nodes["n2"].send_heartbeats()

    connected_leaders = [
        node_id
        for node_id in ("n1", "n2", "n3", "n4")
        if nodes[node_id].role is Role.LEADER
    ]
    assert connected_leaders == ["n2"]
    assert machines["n0"].applied_index == 1

    network.heal()
    nodes["n2"].send_heartbeats()

    assert nodes["n0"].role is Role.FOLLOWER
    assert nodes["n0"].current_term == nodes["n2"].current_term
    assert nodes["n0"].leader_id == "n2"
    assert nodes["n0"].commit_index == 2
    assert machines["n0"].applied_index == 2
    assert [event["msg_id"] for event in machines["n0"].log] == ["m1", "m3"]
    assert [event["msg_id"] for event in machines["n2"].log] == ["m1", "m3"]


def test_retried_msg_id_is_deduplicated_after_leader_change():
    clock, network, nodes, machines = cluster()
    elect_leader(clock, nodes)
    command = request_command("m1")
    assert nodes["n0"].propose(command)["code"] == 200
    nodes["n0"].send_heartbeats()
    assert all(machine.applied_index == 1 for machine in machines.values())

    isolate(network, "n0", nodes)
    clock.advance(1.0)
    nodes["n2"].tick()
    assert nodes["n2"].role is Role.LEADER

    retried = nodes["n2"].propose(command)
    assert retried["code"] == 200
    assert retried["deduplicated"] is True
    nodes["n2"].send_heartbeats()

    network.heal()
    nodes["n2"].send_heartbeats()

    for node_id in ("n1", "n2", "n3", "n4"):
        assert machines[node_id].applied_index == 1
        assert [event["msg_id"] for event in machines[node_id].log] == ["m1"]


def test_raft_logs_carry_timestamp_component_and_msg_id(caplog):
    clock, network, nodes, _ = cluster()
    with caplog.at_level(logging.INFO, logger="sdcal.raft.node"):
        elect_leader(clock, nodes)

    record = next(
        item
        for item in caplog.records
        if item.name == "sdcal.raft.node" and "became leader" in item.getMessage()
    )
    line = _formatter().format(record)
    assert re.match(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+0000 INFO "
        r"sdcal\.raft\.node became leader node_id=n0 term=1",
        line,
    )

    isolate(network, "n0", nodes)
    with caplog.at_level(logging.WARNING, logger="sdcal.raft.node"):
        assert nodes["n0"].propose(request_command("m2")) is None

    pending = next(
        item
        for item in caplog.records
        if item.name == "sdcal.raft.node"
        and "command pending without quorum" in item.getMessage()
    )
    assert "msg_id=m2" in pending.getMessage()
