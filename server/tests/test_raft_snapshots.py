import json
import random

from server.ray_app.consensus.clock import FakeClock
from server.ray_app.consensus.messages import InstallSnapshot, LogEntry, RequestVote
from server.ray_app.consensus.node import RaftNode, Role
from server.ray_app.consensus.storage import SQLiteStorage
from server.ray_app.consensus.transport import InMemoryNetwork


class Counter:
    """Tiny deterministic state machine with a JSON snapshot format."""

    def __init__(self):
        self.value = 0

    def apply(self, command):
        self.value += int(command.get("delta", 1))
        return {"value": self.value}

    def snapshot(self) -> bytes:
        return json.dumps(
            {"value": self.value}, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")

    def restore(self, data: bytes) -> None:
        self.value = int(json.loads(data.decode("utf-8"))["value"])


def cluster(size: int = 5, seed: int = 7):
    clock = FakeClock()
    network = InMemoryNetwork()
    nodes = {}
    machines = {}
    for index in range(size):
        node_id = f"n{index}"
        peers = [f"n{other}" for other in range(size) if other != index]
        machine = Counter()
        node = RaftNode(
            node_id,
            peers,
            network,
            clock=clock,
            random_source=random.Random(seed + index),
            apply_command=machine.apply,
            snapshot_provider=machine.snapshot,
            snapshot_restorer=machine.restore,
        )
        nodes[node_id] = node
        machines[node_id] = machine
        network.register(node_id, node.handle_message)
    return clock, network, nodes, machines


def elect_leader(clock, nodes, leader_id: str = "n0"):
    clock.advance(1.0)
    nodes[leader_id].tick()
    assert nodes[leader_id].role is Role.LEADER


def test_take_snapshot_compacts_applied_prefix():
    clock, _, nodes, machines = cluster()
    elect_leader(clock, nodes)
    for _ in range(3):
        assert nodes["n0"].propose({"delta": 1}) is not None

    assert nodes["n0"].take_snapshot() is True
    assert nodes["n0"].log.snapshot_index == 3
    assert nodes["n0"].log.snapshot_term == 1
    assert nodes["n0"].log.entries == []
    assert nodes["n0"].status()["snapshot_index"] == 3
    assert nodes["n0"].take_snapshot() is False
    assert machines["n0"].value == 3


def test_lagging_follower_installs_snapshot_and_catches_up():
    clock, network, nodes, machines = cluster()
    elect_leader(clock, nodes)
    network.cut("n0", "n4")
    for _ in range(3):
        assert nodes["n0"].propose({"delta": 1}) is not None
    assert machines["n4"].value == 0

    assert nodes["n0"].take_snapshot() is True
    network.heal()
    assert nodes["n0"].propose({"delta": 1}) is not None
    nodes["n0"].send_heartbeats()

    assert nodes["n4"].log.snapshot_index == 3
    assert nodes["n4"].log.snapshot_term == 1
    assert nodes["n4"].log.entries == [LogEntry(4, 1, {"delta": 1})]
    assert nodes["n4"].commit_index == 4
    assert nodes["n4"].last_applied == 4
    assert machines["n4"].value == 4


def test_stale_snapshot_term_is_rejected():
    _, _, nodes, _ = cluster()
    node = nodes["n0"]
    node.handle_request_vote(
        RequestVote(term=2, candidate_id="n1", last_log_index=0, last_log_term=0)
    )
    response = node.handle_install_snapshot(
        InstallSnapshot(
            term=1,
            leader_id="n1",
            last_included_index=1,
            last_included_term=1,
            data=b'{"value":1}',
        )
    )
    assert response.success is False
    assert node.log.snapshot_index == 0


def test_snapshot_below_commit_index_is_ignored():
    clock, _, nodes, machines = cluster()
    elect_leader(clock, nodes)
    assert nodes["n0"].propose({"delta": 1}) is not None
    nodes["n0"].send_heartbeats()
    follower = nodes["n1"]
    assert follower.commit_index == 1
    before = follower.log.entries

    response = follower.handle_install_snapshot(
        InstallSnapshot(
            term=1,
            leader_id="n0",
            last_included_index=1,
            last_included_term=1,
            data=b'{"value":99}',
        )
    )
    assert response.success is True
    assert follower.log.entries == before
    assert machines["n1"].value == 1


def test_save_snapshot_drops_compacted_entries(tmp_path):
    path = tmp_path / "raft.db"
    with SQLiteStorage(path) as storage:
        storage.save_meta(1, "n0")
        storage.save_log(
            [
                LogEntry(1, 1, {"msg_id": "m1"}),
                LogEntry(2, 1, {"msg_id": "m2"}),
                LogEntry(3, 1, {"msg_id": "m3"}),
            ]
        )
        storage.save_snapshot(2, 1, b'{"value":2}')

        assert storage.load_snapshot() == (2, 1, b'{"value":2}')
        assert storage.load_meta()["snapshot_index"] == 2
        assert storage.load_log() == [LogEntry(3, 1, {"msg_id": "m3"})]


def test_snapshot_survives_restart_and_restores_state(tmp_path):
    path = tmp_path / "raft.db"
    machine = Counter()
    storage = SQLiteStorage(path)
    node = RaftNode(
        "n0",
        [],
        InMemoryNetwork(),
        clock=FakeClock(),
        apply_command=machine.apply,
        storage=storage,
        snapshot_provider=machine.snapshot,
        snapshot_restorer=machine.restore,
    )
    node.start_election()
    assert node.propose({"delta": 2}) == {"value": 2}
    assert node.take_snapshot() is True
    storage.close()

    storage = SQLiteStorage(path)
    try:
        assert storage.load_snapshot() == (1, 1, b'{"value":2}')
        restored = Counter()
        restarted = RaftNode(
            "n0",
            [],
            InMemoryNetwork(),
            clock=FakeClock(),
            apply_command=restored.apply,
            storage=storage,
            snapshot_provider=restored.snapshot,
            snapshot_restorer=restored.restore,
        )
        assert restarted.log.snapshot_index == 1
        assert restarted.log.snapshot_term == 1
        assert restarted.log.entries == []
        assert restarted.commit_index == 1
        assert restarted.last_applied == 1
        assert restored.value == 2
    finally:
        storage.close()
