import random

from server.ray_app.consensus.clock import FakeClock
from server.ray_app.consensus.messages import AppendEntries, LogEntry, PreVote
from server.ray_app.consensus.node import RaftNode, Role
from server.ray_app.consensus.storage import SQLiteStorage
from server.ray_app.consensus.transport import InMemoryNetwork


def cluster(size: int = 5):
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
            election_timeout=(0.2, 0.2),
            heartbeat_interval=0.05,
            random_source=random.Random(index),
        )
        nodes[node_id] = node
        network.register(node_id, node.handle_message)
    return clock, network, nodes


def elect_leader(clock, nodes, leader_id: str = "n0"):
    clock.advance(1.0)
    nodes[leader_id].tick()
    assert nodes[leader_id].role is Role.LEADER
    return nodes[leader_id]


def send_heartbeat(node, term: int = 1, leader_id: str = "n0"):
    return node.handle_append_entries(
        AppendEntries(
            term=term,
            leader_id=leader_id,
            prev_log_index=node.log.last_index,
            prev_log_term=node.log.last_term,
            entries=[],
            leader_commit=node.commit_index,
        )
    )


def replace_with_restarted_node(network, nodes, node_id, term, path):
    network.unregister(node_id)
    storage = SQLiteStorage(path)
    storage.save_meta(term, "n0")
    peers = [peer for peer in nodes if peer != node_id]
    node = RaftNode(
        node_id,
        peers,
        network,
        clock=nodes[node_id].clock,
        election_timeout=(0.2, 0.2),
        heartbeat_interval=0.05,
        random_source=random.Random(99),
        storage=storage,
    )
    nodes[node_id] = node
    network.register(node_id, node.handle_message)
    return node, storage


def test_pre_vote_quorum_is_required_before_incrementing_term():
    clock, network, nodes = cluster()
    for peer in ("n2", "n3", "n4"):
        network.cut("n0", peer)

    clock.advance(1.0)
    nodes["n0"].tick()

    assert nodes["n0"].role is Role.PRE_CANDIDATE
    assert nodes["n0"].current_term == 0
    assert nodes["n1"].current_term == 0
    assert nodes["n1"].voted_for is None

    network.heal()
    clock.advance(1.0)
    nodes["n0"].tick()
    assert nodes["n0"].role is Role.LEADER
    assert nodes["n0"].current_term == 1


def test_pre_vote_does_not_mutate_voter_term_or_vote():
    _, _, nodes = cluster()
    voter = nodes["n1"]
    voter.current_term = 4
    voter.voted_for = "n2"

    response = voter.handle_message(
        PreVote(
            prospective_term=5,
            candidate_id="n0",
            last_log_index=0,
            last_log_term=0,
        )
    )

    assert response.vote_granted is True
    assert voter.current_term == 4
    assert voter.voted_for == "n2"


def test_voter_refuses_pre_vote_during_the_leader_lease():
    clock, _, nodes = cluster()
    elect_leader(clock, nodes)
    follower = nodes["n1"]
    request = PreVote(
        prospective_term=2,
        candidate_id="n2",
        last_log_index=0,
        last_log_term=0,
    )

    denied = follower.handle_message(request)
    assert denied.vote_granted is False

    clock.advance(0.21)
    granted = follower.handle_message(request)
    assert granted.vote_granted is True


def test_leader_refuses_pre_vote():
    clock, _, nodes = cluster()
    leader = elect_leader(clock, nodes)

    response = leader.handle_message(
        PreVote(
            prospective_term=2,
            candidate_id="n1",
            last_log_index=0,
            last_log_term=0,
        )
    )

    assert response.vote_granted is False
    assert leader.role is Role.LEADER


def test_pre_vote_requires_an_up_to_date_candidate_log():
    _, _, nodes = cluster()
    voter = nodes["n1"]
    voter.log.append(LogEntry(index=1, term=2, command={"msg_id": "m1"}))

    response = voter.handle_message(
        PreVote(
            prospective_term=3,
            candidate_id="n0",
            last_log_index=0,
            last_log_term=1,
        )
    )

    assert response.vote_granted is False


def test_isolated_replica_does_not_depose_a_healthy_leader():
    clock, network, nodes = cluster()
    leader = elect_leader(clock, nodes)
    for peer in ("n1", "n3", "n4"):
        network.cut("n2", peer)

    clock.advance(0.21)
    nodes["n2"].tick()

    assert nodes["n2"].role is Role.PRE_CANDIDATE
    assert nodes["n2"].current_term == 1
    assert leader.role is Role.LEADER
    assert leader.current_term == 1


def test_restarted_replica_does_not_depose_a_healthy_leader(tmp_path):
    clock, network, nodes = cluster()
    leader = elect_leader(clock, nodes)
    restarted, storage = replace_with_restarted_node(
        network, nodes, "n2", 1, tmp_path / "n2.db"
    )
    try:
        clock.advance(0.21)
        for peer in ("n1", "n3", "n4"):
            send_heartbeat(nodes[peer])
        restarted.tick()

        assert restarted.role is Role.PRE_CANDIDATE
        assert restarted.current_term == 1
        assert leader.role is Role.LEADER
        assert leader.current_term == 1
    finally:
        storage.close()


def test_restarted_replica_with_a_higher_term_does_not_depose_a_healthy_leader(
    tmp_path,
):
    clock, network, nodes = cluster()
    leader = elect_leader(clock, nodes)
    restarted, storage = replace_with_restarted_node(
        network, nodes, "n2", 5, tmp_path / "n2.db"
    )
    try:
        clock.advance(0.21)
        for peer in ("n1", "n3", "n4"):
            send_heartbeat(nodes[peer])
        restarted.tick()

        assert restarted.role is Role.PRE_CANDIDATE
        assert restarted.current_term == 5
        assert leader.role is Role.LEADER
        assert leader.current_term == 1
    finally:
        storage.close()
