import time
import uuid

import pytest

ray = pytest.importorskip("ray")

from server.ray_app.raft_actor import start_raft_cluster, stop_raft_cluster
from server.ray_app.raft_cluster import RaftCluster
from server.ray_app.raft_ticker import start_tickers, stop_tickers


def request_command(index: int, title: str):
    token = f"01J000000000000000000000{index:02d}"
    return {
        "deployment_id": "dep1",
        "group_id": "team",
        "msg_id": token,
        "type": "REQUEST",
        "uid": f"alice:{token}",
        "sender_id": "alice",
        "key_id": "key1",
        "sequence": 1,
        "seen_sequence": 0,
        "client_sent_at": "2026-11-21T00:00:00Z",
        "payload": {"title": title},
    }


def wait_for_leader(cluster: RaftCluster, timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leader = cluster.leader()
        if leader is not None:
            return leader
        time.sleep(0.05)
    return None


def wait_for_applied(cluster: RaftCluster, index: int, timeout: float = 15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        statuses = cluster.statuses()
        if statuses and all(
            status.get("last_applied", 0) >= index for status in statuses.values()
        ):
            return True
        time.sleep(0.05)
    return False


@pytest.fixture(scope="module")
def ray_runtime():
    initialized = ray.is_initialized()
    if not initialized:
        ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    yield
    if not initialized and ray.is_initialized():
        ray.shutdown()


@pytest.fixture
def ticking_cluster(ray_runtime, tmp_path):
    namespace = f"sdcal-test-{uuid.uuid4().hex}"
    node_ids = [f"n{index}" for index in range(5)]
    db_paths = {node_id: str(tmp_path / f"{node_id}.db") for node_id in node_ids}
    actors = start_raft_cluster(node_ids, db_paths, namespace=namespace)
    tickers = start_tickers(actors, interval=0.05, namespace=namespace)
    cluster = RaftCluster(actors, namespace=namespace)
    try:
        yield cluster, actors
    finally:
        stop_tickers(tickers)
        stop_raft_cluster(actors)


def test_ticker_elects_leader_without_manual_trigger(ticking_cluster):
    cluster, actors = ticking_cluster
    leader_id = wait_for_leader(cluster)
    assert leader_id is not None
    statuses = cluster.statuses()
    assert statuses[leader_id]["role"] == "leader"
    assert statuses[leader_id]["term"] >= 1
    leaders = [
        node_id
        for node_id, status in statuses.items()
        if status["role"] == "leader"
    ]
    assert leaders == [leader_id]


def test_cluster_propose_commits_and_reaches_every_replica(ticking_cluster):
    cluster, actors = ticking_cluster
    assert wait_for_leader(cluster) is not None

    result = cluster.propose(request_command(41, "Reunión de equipo"))
    assert result["code"] == 200
    assert wait_for_applied(cluster, 1)
    statuses = cluster.statuses()
    assert all(status["last_applied"] == 1 for status in statuses.values())


def test_cluster_keeps_committing_with_two_replicas_down(ticking_cluster):
    cluster, actors = ticking_cluster
    leader_id = wait_for_leader(cluster)
    assert leader_id is not None
    followers = [node_id for node_id in actors if node_id != leader_id]
    for node_id in followers[:2]:
        ray.kill(actors[node_id])

    result = cluster.propose(request_command(42, "Caos"))
    assert result["code"] == 200
    assert len(cluster.statuses()) == 3


def test_cluster_refuses_to_commit_without_quorum(ticking_cluster):
    cluster, actors = ticking_cluster
    leader_id = wait_for_leader(cluster)
    assert leader_id is not None
    followers = [node_id for node_id in actors if node_id != leader_id]
    for node_id in followers[:3]:
        ray.kill(actors[node_id])

    outcome = None
    try:
        outcome = cluster.propose(request_command(43, "Sin quorum"))
    except Exception:
        outcome = None
    assert outcome is None
