import random

import pytest

from server.ray_app.consensus.clock import FakeClock
from server.ray_app.consensus.messages import AppendEntries, LogEntry, RequestVote
from server.ray_app.consensus.node import RaftNode, Role
from server.ray_app.consensus.transport import InMemoryNetwork


def cluster(size: int = 5, seed: int = 7):
    clock = FakeClock()
    network = InMemoryNetwork()
    nodes = {}
    for index in range(size):
        node_id = f"n{index}"
        peers = [f"n{other}" for other in range(size) if other != index]
        node = RaftNode(
            node_id,
            peers,
            network,
            clock=clock,
            random_source=random.Random(seed + index),
        )
        nodes[node_id] = node
        network.register(node_id, node.handle_message)
    return clock, network, nodes


def leaders(nodes):
    return [node for node in nodes.values() if node.role is Role.LEADER]


def test_elects_a_single_leader():
    clock, _, nodes = cluster()
    clock.advance(1.0)
    nodes["n0"].tick()
    assert nodes["n0"].role is Role.LEADER
    assert nodes["n0"].current_term == 1
    assert nodes["n0"].leader_id == "n0"
    assert all(node.leader_id == "n0" for node in nodes.values())


def test_only_one_leader_when_every_node_ticks():
    clock, _, nodes = cluster()
    clock.advance(1.0)
    for node in nodes.values():
        node.tick()
    assert len(leaders(nodes)) == 1


def test_pre_candidate_without_quorum_does_not_change_term():
    clock, network, nodes = cluster()
    for peer in ("n2", "n3", "n4"):
        network.cut("n0", peer)
    clock.advance(1.0)
    nodes["n0"].tick()
    assert nodes["n0"].role is Role.PRE_CANDIDATE
    assert nodes["n0"].current_term == 0


def test_split_vote_is_resolved_after_healing():
    clock, network, nodes = cluster()
    ids = list(nodes)
    for left in range(len(ids)):
        for right in range(left + 1, len(ids)):
            network.cut(ids[left], ids[right])
    clock.advance(1.0)
    nodes["n0"].tick()
    nodes["n1"].tick()
    assert nodes["n0"].role is Role.PRE_CANDIDATE
    assert nodes["n1"].role is Role.PRE_CANDIDATE
    assert nodes["n0"].current_term == 0
    assert nodes["n1"].current_term == 0

    network.heal()
    clock.advance(1.0)
    nodes["n0"].tick()
    assert nodes["n0"].role is Role.LEADER
    assert nodes["n0"].current_term == 1


def test_follower_elects_new_leader_after_leader_failure():
    clock, network, nodes = cluster()
    clock.advance(1.0)
    nodes["n0"].tick()
    assert nodes["n0"].role is Role.LEADER

    network.unregister("n0")
    clock.advance(1.0)
    nodes["n1"].tick()
    assert nodes["n1"].role is Role.LEADER
    assert nodes["n1"].current_term == 2


def test_stale_candidate_is_rejected():
    _, _, nodes = cluster()
    follower = nodes["n0"]
    follower.log.append(LogEntry(index=1, term=2, command={"msg_id": "m1"}))
    response = follower.handle_request_vote(
        RequestVote(term=3, candidate_id="n1", last_log_index=0, last_log_term=0)
    )
    assert response.vote_granted is False


def test_voter_grants_once_per_term():
    _, _, nodes = cluster()
    voter = nodes["n2"]
    first = voter.handle_request_vote(RequestVote(1, "n0", 0, 0))
    second = voter.handle_request_vote(RequestVote(1, "n1", 0, 0))
    assert first.vote_granted is True
    assert second.vote_granted is False


def test_leader_steps_down_on_higher_term():
    clock, _, nodes = cluster()
    clock.advance(1.0)
    nodes["n0"].tick()
    response = nodes["n0"].handle_append_entries(
        AppendEntries(
            term=2,
            leader_id="n1",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )
    )
    assert response.success is True
    assert nodes["n0"].role is Role.FOLLOWER
    assert nodes["n0"].current_term == 2
    assert nodes["n0"].leader_id == "n1"


def test_heartbeat_keeps_followers_passive():
    clock, _, nodes = cluster()
    clock.advance(1.0)
    nodes["n0"].tick()
    clock.advance(0.1)
    nodes["n1"].tick()
    assert nodes["n1"].role is Role.FOLLOWER


def test_status_reports_election_state():
    clock, _, nodes = cluster()
    clock.advance(1.0)
    nodes["n0"].tick()
    status = nodes["n0"].status()
    assert status["role"] == "leader"
    assert status["term"] == 1
    assert status["leader_id"] == "n0"


class FailingStorage:
    def load_meta(self):
        return {}

    def save_meta(self, *args, **kwargs):
        pass

    def load_log(self):
        return []

    def save_log(self, *args, **kwargs):
        raise OSError("disk full")

    def save_snapshot(self, *args, **kwargs):
        pass

    def load_snapshot(self):
        return None


def test_failed_log_persistence_does_not_leave_a_phantom_entry():
    node = RaftNode(
        "n0",
        [],
        InMemoryNetwork(),
        clock=FakeClock(),
        storage=FailingStorage(),
    )
    node.start_election()
    assert node.role is Role.LEADER

    with pytest.raises(OSError, match="disk full"):
        node.propose({"msg_id": "m1"})

    assert node.log.last_index == 0


def test_failed_replication_persistence_does_not_mutate_the_follower_log():
    node = RaftNode(
        "n0",
        [],
        InMemoryNetwork(),
        clock=FakeClock(),
        storage=FailingStorage(),
    )
    message = AppendEntries(
        term=1,
        leader_id="n1",
        prev_log_index=0,
        prev_log_term=0,
        entries=[LogEntry(index=1, term=1, command={"msg_id": "m1"})],
        leader_commit=0,
    )

    with pytest.raises(OSError, match="disk full"):
        node.handle_append_entries(message)

    assert node.log.last_index == 0
    assert node.commit_index == 0


def test_failed_replication_persistence_keeps_the_previous_suffix():
    node = RaftNode(
        "n0",
        [],
        InMemoryNetwork(),
        clock=FakeClock(),
        storage=FailingStorage(),
    )
    node.log.append(LogEntry(index=1, term=1, command={"msg_id": "old"}))
    message = AppendEntries(
        term=2,
        leader_id="n1",
        prev_log_index=0,
        prev_log_term=0,
        entries=[LogEntry(index=1, term=2, command={"msg_id": "new"})],
        leader_commit=0,
    )

    with pytest.raises(OSError, match="disk full"):
        node.handle_append_entries(message)

    assert [entry.command["msg_id"] for entry in node.log.entries] == ["old"]
