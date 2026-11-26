from server.ray_app.consensus.clock import FakeClock
from server.ray_app.consensus.messages import AppendEntries, LogEntry, RequestVote
from server.ray_app.consensus.node import RaftNode, Role
from server.ray_app.consensus.storage import SQLiteStorage
from server.ray_app.consensus.transport import InMemoryNetwork


def entry(index: int, term: int = 1, msg_id: str = "") -> LogEntry:
    return LogEntry(index=index, term=term, command={"msg_id": msg_id or f"m{index}"})


def open_node(path, node_id="n0", peers=(), apply_command=None):
    storage = SQLiteStorage(path)
    node = RaftNode(
        node_id,
        list(peers),
        InMemoryNetwork(),
        clock=FakeClock(),
        apply_command=apply_command,
        storage=storage,
    )
    return node, storage


def test_empty_storage_has_no_meta_or_log(tmp_path):
    with SQLiteStorage(tmp_path / "raft.db") as storage:
        assert storage.load_meta() == {}
        assert storage.load_log() == []


def test_meta_round_trip(tmp_path):
    with SQLiteStorage(tmp_path / "raft.db") as storage:
        storage.save_meta(3, "n2")
        assert storage.load_meta() == {
            "current_term": 3,
            "voted_for": "n2",
            "snapshot_index": 0,
            "snapshot_term": 0,
        }

        storage.save_meta(4, None, snapshot_index=2, snapshot_term=2)
        assert storage.load_meta() == {
            "current_term": 4,
            "voted_for": None,
            "snapshot_index": 2,
            "snapshot_term": 2,
        }


def test_log_round_trip(tmp_path):
    with SQLiteStorage(tmp_path / "raft.db") as storage:
        storage.save_log([entry(1), entry(2, term=2)])
        assert storage.load_log() == [entry(1), entry(2, term=2)]


def test_save_log_appends_and_replaces(tmp_path):
    with SQLiteStorage(tmp_path / "raft.db") as storage:
        storage.save_log([entry(1), entry(2)])
        storage.save_log([entry(3, term=2)])
        assert [item.index for item in storage.load_log()] == [1, 2, 3]

        storage.save_log([entry(1, term=2), entry(2, term=2)], replace=True)
        assert storage.load_log() == [entry(1, term=2), entry(2, term=2)]


def test_save_log_replaces_only_a_conflicting_suffix(tmp_path):
    with SQLiteStorage(tmp_path / "raft.db") as storage:
        storage.save_log([entry(1), entry(2), entry(3)])

        storage.save_log(
            [entry(2, term=2), entry(3, term=2)],
            truncate_from=2,
        )

        assert storage.load_log() == [entry(1), entry(2, term=2), entry(3, term=2)]


def test_restart_restores_term_vote_and_log(tmp_path):
    path = tmp_path / "raft.db"
    node, storage = open_node(path, apply_command=lambda command: command)
    node.start_election()
    assert node.role is Role.LEADER
    assert node.propose({"msg_id": "m1"}) == {"msg_id": "m1"}
    assert node.commit_index == 1
    storage.close()

    restarted, storage = open_node(path)
    try:
        assert restarted.current_term == 1
        assert restarted.voted_for == "n0"
        assert restarted.log.entries == [entry(1, msg_id="m1")]
        assert restarted.commit_index == 0
        assert restarted.last_applied == 0
        assert restarted.role is Role.FOLLOWER
    finally:
        storage.close()


def test_granted_vote_is_persisted_before_reply(tmp_path):
    path = tmp_path / "raft.db"
    node, storage = open_node(path, node_id="n2", peers=("n0", "n1"))
    response = node.handle_request_vote(
        RequestVote(term=2, candidate_id="n1", last_log_index=0, last_log_term=0)
    )
    assert response.vote_granted is True
    storage.close()

    restarted, storage = open_node(path, node_id="n2", peers=("n0", "n1"))
    try:
        assert restarted.current_term == 2
        assert restarted.voted_for == "n1"
        denied = restarted.handle_request_vote(
            RequestVote(term=2, candidate_id="n0", last_log_index=0, last_log_term=0)
        )
        assert denied.vote_granted is False
    finally:
        storage.close()


def test_follower_append_is_persisted(tmp_path):
    path = tmp_path / "raft.db"
    node, storage = open_node(path, node_id="n3", peers=("n0",))
    response = node.handle_append_entries(
        AppendEntries(
            term=1,
            leader_id="n0",
            prev_log_index=0,
            prev_log_term=0,
            entries=[entry(1), entry(2)],
            leader_commit=1,
        )
    )
    assert response.success is True
    storage.close()

    restarted, storage = open_node(path, node_id="n3", peers=("n0",))
    try:
        assert restarted.current_term == 1
        assert restarted.log.entries == [entry(1), entry(2)]
    finally:
        storage.close()


def test_follower_truncation_is_persisted(tmp_path):
    path = tmp_path / "raft.db"
    node, storage = open_node(path, node_id="n3", peers=("n0",))
    assert node.handle_append_entries(
        AppendEntries(
            term=1,
            leader_id="n0",
            prev_log_index=0,
            prev_log_term=0,
            entries=[entry(1), entry(2), entry(3)],
        )
    ).success
    assert node.handle_append_entries(
        AppendEntries(
            term=2,
            leader_id="n0",
            prev_log_index=1,
            prev_log_term=1,
            entries=[entry(2, term=2), entry(3, term=2)],
        )
    ).success
    storage.close()

    restarted, storage = open_node(path, node_id="n3", peers=("n0",))
    try:
        assert restarted.log.entries == [entry(1), entry(2, term=2), entry(3, term=2)]
    finally:
        storage.close()


def test_restart_restores_snapshot_base(tmp_path):
    path = tmp_path / "raft.db"
    with SQLiteStorage(path) as storage:
        storage.save_meta(2, "n1", snapshot_index=5, snapshot_term=1)
        storage.save_log([entry(6, term=2), entry(7, term=2)])

    node, storage = open_node(path, node_id="n1", peers=("n0",))
    try:
        assert node.log.snapshot_index == 5
        assert node.log.snapshot_term == 1
        assert node.log.last_index == 7
        assert node.log.term_at(5) == 1
    finally:
        storage.close()


def test_single_node_elects_itself_and_persists(tmp_path):
    path = tmp_path / "raft.db"
    node, storage = open_node(path)
    node.start_election()
    assert node.role is Role.LEADER
    assert node.current_term == 1
    storage.close()

    restarted, storage = open_node(path)
    try:
        assert restarted.current_term == 1
        assert restarted.voted_for == "n0"
    finally:
        storage.close()


def test_save_log_round_trips_non_ascii_commands(tmp_path):
    command = {"msg_id": "m1", "title": "Diseño"}
    with SQLiteStorage(tmp_path / "raft.db") as storage:
        storage.save_log([LogEntry(index=1, term=1, command=command)])

        assert storage.load_log() == [LogEntry(index=1, term=1, command=command)]
