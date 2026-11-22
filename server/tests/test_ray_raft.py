import json
import uuid

import pytest

ray = pytest.importorskip("ray")

from server.ray_app.consensus.transport import TransportError
from server.ray_app.raft_actor import start_raft_cluster, stop_raft_cluster
from server.ray_app.raft_cluster import RaftCluster
from server.ray_app.raft_transport import RayTransport, actor_name


@ray.remote
class EchoActor:
    def handle_message(self, message):
        return message


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


def snapshot_of(actor):
    return json.loads(ray.get(actor.snapshot.remote()).decode("utf-8"))


@pytest.fixture(scope="module")
def ray_runtime():
    initialized = ray.is_initialized()
    if not initialized:
        ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
    yield
    if not initialized and ray.is_initialized():
        ray.shutdown()


@pytest.fixture
def cluster(ray_runtime, tmp_path):
    namespace = f"sdcal-test-{uuid.uuid4().hex}"
    node_ids = [f"n{index}" for index in range(5)]
    db_paths = {node_id: str(tmp_path / f"{node_id}.db") for node_id in node_ids}
    actors = start_raft_cluster(node_ids, db_paths, namespace=namespace)
    try:
        yield namespace, actors
    finally:
        stop_raft_cluster(actors)


def test_actor_name_is_deterministic():
    assert actor_name("n3") == "sdcal-raft-n3"
    assert actor_name("n3", prefix="custom-") == "custom-n3"


def test_ray_transport_reports_missing_actor(ray_runtime):
    transport = RayTransport(
        namespace=f"sdcal-missing-{uuid.uuid4().hex}", timeout=0.1
    )
    with pytest.raises(TransportError):
        transport.send("n0", "n1", {"ping": True})


def test_ray_transport_unblocks_when_named_actor_reappears(ray_runtime):
    namespace = f"sdcal-reappear-{uuid.uuid4().hex}"
    transport = RayTransport(
        namespace=namespace,
        timeout=2.0,
        failure_threshold=1,
        failure_cooldown=60.0,
    )
    target = "transport-reappear"
    with pytest.raises(TransportError):
        transport.send("n0", target, {"ping": True})

    actor = EchoActor.options(
        name=actor_name(target), namespace=namespace
    ).remote()
    try:
        assert transport.send("n0", target, {"ping": True}) == {"ping": True}
    finally:
        ray.kill(actor)


def test_ray_cluster_elects_leader_and_commits(cluster):
    namespace, actors = cluster
    ray.get(actors["n0"].start_election.remote())

    status = ray.get(actors["n0"].status.remote())
    assert status["role"] == "leader"
    assert status["term"] == 1

    result = ray.get(actors["n0"].propose.remote(request_command(11, "Diseño")))
    assert result["code"] == 200
    assert result["deduplicated"] is False
    ray.get(actors["n0"].send_heartbeats.remote())

    for node_id in ("n1", "n2", "n3", "n4"):
        follower = ray.get(actors[node_id].status.remote())
        assert follower["leader_id"] == "n0"
        assert follower["commit_index"] == 1
        assert follower["last_applied"] == 1
        snapshot = snapshot_of(actors[node_id])
        assert snapshot["applied_index"] == 1
        assert "alice:01J00000000000000000000011" in snapshot["events"]

    calendar = RaftCluster(list(actors), namespace=namespace)
    assert calendar.sync(0)["entries"][0]["index"] == 1
    assert calendar.conflicts(
        "alice", "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z"
    )["conflicts"] == []
    assert calendar.availability(["alice", "bob"])["users"] == {
        "alice": "available",
        "bob": "available",
    }


def test_ray_follower_rejects_propose(cluster):
    _, actors = cluster
    ray.get(actors["n0"].start_election.remote())
    with pytest.raises(Exception) as exc_info:
        ray.get(actors["n1"].propose.remote(request_command(21, "Reunión")))
    assert "not the leader" in str(exc_info.value)


def test_sync_rejects_invalid_cursor_before_ray_execution():
    with pytest.raises(ValueError, match="invalid_sync_cursor"):
        RaftCluster([]).sync(-1)


def test_ray_lagging_replica_installs_snapshot(ray_runtime, tmp_path):
    namespace = f"sdcal-test-{uuid.uuid4().hex}"
    node_ids = [f"n{index}" for index in range(5)]
    db_paths = {node_id: str(tmp_path / f"{node_id}.db") for node_id in node_ids}
    actors = start_raft_cluster(
        node_ids[:4],
        db_paths,
        namespace=namespace,
        membership=node_ids,
    )
    try:
        ray.get(actors["n0"].start_election.remote())
        assert ray.get(actors["n0"].status.remote())["role"] == "leader"
        for index in range(1, 4):
            result = ray.get(
                actors["n0"].propose.remote(
                    request_command(index, f"Evento {index}")
                )
            )
            assert result["code"] == 200
        assert ray.get(actors["n0"].take_snapshot.remote()) is True

        actors.update(
            start_raft_cluster(
                ["n4"],
                db_paths,
                namespace=namespace,
                membership=node_ids,
            )
        )
        assert (
            ray.get(
                actors["n0"].propose.remote(request_command(4, "Evento 4"))
            )["code"]
            == 200
        )
        ray.get(actors["n0"].send_heartbeats.remote())

        status = ray.get(actors["n4"].status.remote())
        assert status["snapshot_index"] == 3
        assert status["snapshot_term"] == 1
        assert status["commit_index"] == 4
        assert status["last_applied"] == 4
        snapshot = snapshot_of(actors["n4"])
        assert snapshot["applied_index"] == 4
        assert len(snapshot["events"]) == 4
    finally:
        stop_raft_cluster(actors)
