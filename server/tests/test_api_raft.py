import pytest
from fastapi.testclient import TestClient

from server.ray_app import api
from server.ray_app.raft_cluster import NoLeaderError


class FakeCluster:
    """Stand-in for :class:`RaftCluster` without needing Ray."""

    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.proposed = []

    def propose(self, command):
        self.proposed.append(command)
        if self.error is not None:
            raise self.error
        return self.result

    def status(self):
        return {
            "service": "sd-calendario",
            "middleware": "ray",
            "leader_id": "n0",
            "replicas": {"n0": {"role": "leader", "term": 3}},
        }

    def snapshot(self):
        return {
            "events": {
                "alice:01J00000000000000000000001": {
                    "uid": "alice:01J00000000000000000000001",
                    "state": "CONFIRMED",
                }
            },
            "applied_index": 1,
        }

    def availability(self, users=None, start=None, end=None):
        return {"users": {}, "as_of": 1}

    def conflicts(self, user, start, end, exclude_uid=None):
        return {"user": user, "conflicts": [], "as_of": 1}

    def sync(self, since=0, limit=100):
        return {"entries": [], "next": since, "as_of": 1}


def command(msg_id="01J00000000000000000000002"):
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
        "payload": {"title": "Prueba"},
    }


@pytest.fixture
def client():
    api.set_cluster(None)
    yield TestClient(api.app)
    api.set_cluster(None)


def test_command_is_forwarded_to_the_cluster(client):
    cluster = FakeCluster(
        result={"code": 200, "deduplicated": False, "applied_index": 1, "result": {}}
    )
    api.set_cluster(cluster)

    response = client.post("/v1/commands", json=command())
    assert response.status_code == 200
    assert response.json()["code"] == 200
    assert cluster.proposed == [command()]


def test_command_without_leader_returns_503(client):
    api.set_cluster(FakeCluster(error=NoLeaderError("raft: no leader available")))

    response = client.post("/v1/commands", json=command())
    assert response.status_code == 503
    assert "no leader" in response.json()["detail"]["reason"]


def test_command_without_quorum_returns_503(client):
    api.set_cluster(FakeCluster(result=None))

    response = client.post("/v1/commands", json=command())
    assert response.status_code == 503
    assert "quorum" in response.json()["detail"]["reason"]


def test_domain_error_is_propagated(client):
    api.set_cluster(
        FakeCluster(result={"code": 409, "deduplicated": False, "result": {}})
    )

    response = client.post("/v1/commands", json=command())
    assert response.status_code == 409


def test_reads_are_served_by_the_cluster(client):
    api.set_cluster(FakeCluster())

    status = client.get("/v1/status").json()
    assert status["leader_id"] == "n0"
    assert status["replicas"]["n0"]["term"] == 3

    snapshot = client.get("/v1/kv/*").json()
    assert snapshot["applied_index"] == 1

    event = client.get("/v1/kv/alice:01J00000000000000000000001").json()
    assert event["found"] is True

    missing = client.get("/v1/kv/alice:missing").json()
    assert missing["found"] is False

    assert client.get("/v1/availability").json()["as_of"] == 1
    assert client.get("/v1/conflicts?user=alice&start=a&end=b").json()["conflicts"] == []
    assert client.get("/v1/sync").json()["next"] == 0


def test_legacy_path_serves_without_cluster(client):
    response = client.post("/v1/commands", json=command())
    assert response.status_code == 200
    assert client.get("/v1/status").json()["middleware"] == "ray"
