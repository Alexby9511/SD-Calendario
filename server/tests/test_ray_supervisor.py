import time
import uuid

import pytest

ray = pytest.importorskip("ray")

from server.ray_app.raft_actor import start_raft_cluster, stop_raft_cluster
from server.ray_app.raft_cluster import RaftCluster
from server.ray_app.raft_supervisor import start_supervisor, stop_supervisor
from server.ray_app.raft_ticker import start_tickers, stop_tickers
from server.ray_app.raft_supervisor import _SupervisorReplica


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


def wait_for_leader(cluster: RaftCluster, timeout: float = 20.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leader = cluster.leader()
        if leader is not None:
            return leader
        time.sleep(0.05)
    return None


def wait_until(predicate, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.2)
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
def supervised_cluster(ray_runtime, tmp_path):
    namespace = f"sdcal-test-{uuid.uuid4().hex}"
    node_ids = [f"n{index}" for index in range(5)]
    db_paths = {node_id: str(tmp_path / f"{node_id}.db") for node_id in node_ids}
    actors = start_raft_cluster(node_ids, db_paths, namespace=namespace)
    tickers = start_tickers(actors, namespace=namespace)
    supervisor = start_supervisor(node_ids, db_paths, namespace=namespace)
    cluster = RaftCluster(actors, namespace=namespace)
    try:
        yield cluster, actors, namespace
    finally:
        stop_supervisor(supervisor)
        stop_tickers(tickers)
        stop_raft_cluster(actors)


def test_supervisor_recreates_a_killed_replica(supervised_cluster):
    cluster, actors, namespace = supervised_cluster
    leader_id = wait_for_leader(cluster)
    assert leader_id is not None
    leader_term = cluster.statuses()[leader_id]["term"]
    victim = [node_id for node_id in actors if node_id != leader_id][0]

    ray.kill(actors[victim])
    assert wait_until(lambda: victim in cluster.statuses())
    assert wait_until(
        lambda: cluster.statuses().get(victim, {}).get("role") == "follower"
        and cluster.statuses().get(victim, {}).get("leader_id") == leader_id
    )

    statuses = cluster.statuses()
    assert statuses[victim]["leader_id"] == leader_id
    assert statuses[leader_id]["role"] == "leader"
    assert statuses[leader_id]["term"] == leader_term


def test_recreated_replica_catches_up_with_the_leader(supervised_cluster):
    cluster, actors, namespace = supervised_cluster
    leader_id = wait_for_leader(cluster)
    assert leader_id is not None
    victim = [node_id for node_id in actors if node_id != leader_id][0]

    ray.kill(actors[victim])
    for index in range(1, 4):
        result = cluster.propose(request_command(index, f"Evento {index}"))
        assert result["code"] == 200

    def caught_up():
        statuses = cluster.statuses()
        return (
            victim in statuses
            and statuses[victim]["last_applied"] >= 3
            and statuses[victim]["commit_index"] >= 3
        )

    assert wait_until(caught_up)
    statuses = cluster.statuses()
    assert statuses[victim]["last_applied"] == statuses[leader_id]["last_applied"]


def test_cluster_keeps_committing_while_two_replicas_are_down(supervised_cluster):
    cluster, actors, namespace = supervised_cluster
    leader_id = wait_for_leader(cluster)
    assert leader_id is not None
    followers = [node_id for node_id in actors if node_id != leader_id]
    for node_id in followers[:2]:
        ray.kill(actors[node_id])

    result = cluster.propose(request_command(71, "Sin dos réplicas"))
    assert result["code"] == 200
    assert len(cluster.statuses()) >= 3


def test_fresh_client_attaches_to_the_running_replicas(supervised_cluster):
    cluster, actors, namespace = supervised_cluster
    assert wait_for_leader(cluster) is not None

    # A restarted API process only knows the replica names.
    fresh = RaftCluster(list(actors), namespace=namespace)
    result = fresh.propose(request_command(81, "Tras reiniciar la API"))
    assert result["code"] == 200
    assert fresh.leader() is not None


def test_strict_supervisor_rejects_missing_placement_targets():
    with pytest.raises(ValueError, match="requires placements"):
        _SupervisorReplica(
            ["n0"],
            {"n0": "n0.db"},
            "sdcal-test",
            lambda: None,
            0.1,
            0.1,
            {},
            0.1,
            "ticker-",
            strict_placement=True,
        )


def test_unresponsive_actor_is_killed_after_health_threshold(monkeypatch):
    class HangingActor:
        class StatusMethod:
            def remote(self):
                return "status-ref"

        status = StatusMethod()

    class FakeRay:
        registered = True
        killed = 0

        def get_actor(self, name, namespace=None):
            if self.registered:
                return actor
            raise RuntimeError("actor is no longer registered")

        def get(self, ref, timeout=None):
            raise TimeoutError("actor status timed out")

        def kill(self, handle, no_restart=True):
            assert handle is actor
            assert no_restart is True
            self.killed += 1
            self.registered = False

    actor = HangingActor()
    fake_ray = FakeRay()
    monkeypatch.setattr("server.ray_app.raft_supervisor.ray", fake_ray)
    supervisor = _SupervisorReplica(
        ["n0"],
        {"n0": "n0.db"},
        "sdcal-test",
        lambda: None,
        0.1,
        0.1,
        {},
        0.1,
        "ticker-",
        unhealthy_threshold=2,
    )
    recreated = []
    monkeypatch.setattr(supervisor, "_recreate", lambda node_id: recreated.append(node_id) or True)
    monkeypatch.setattr(supervisor, "_ensure_ticker", lambda _node_id: False)

    assert supervisor.check() == []
    assert fake_ray.killed == 0
    assert supervisor.check() == ["n0"]
    assert fake_ray.killed == 1
    assert recreated == ["n0"]
