import json

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
            "alive_replicas": 1,
            "quorum_size": 1,
            "quorum_available": True,
        }

    def snapshot(self):
        return {
            "events": {
                "alice:01J00000000000000000000001": {
                    "uid": "alice:01J00000000000000000000001",
                    "state": "CONFIRMED",
                    "organizer": "alice",
                    "participants": [],
                    "visibility": "private",
                    "payload": {"title": "Privado"},
                },
                "bob:01J00000000000000000000002": {
                    "uid": "bob:01J00000000000000000000002",
                    "state": "CONFIRMED",
                    "organizer": "bob",
                    "participants": [],
                    "visibility": "public",
                    "payload": {"title": "Público"},
                },
            },
            "applied_index": 1,
        }

    def availability(self, users=None, start=None, end=None):
        selected = users or ["alice"]
        return {
            "users": {
                user: "busy" if user == "alice" else "available"
                for user in selected
            },
            "as_of": 1,
        }

    def conflicts(self, user, start, end, exclude_uid=None):
        return {"user": user, "conflicts": [], "as_of": 1}

    def sync(self, since=0, limit=100, user_id=None):
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
def client(monkeypatch):
    monkeypatch.setenv(
        "SDCAL_API_TOKENS",
        json.dumps({"alice": "a" * 40, "bob": "b" * 40}),
    )
    api.set_cluster(None)
    yield TestClient(api.app)
    api.set_cluster(None)


def headers(user="alice"):
    token = "a" * 40 if user == "alice" else "b" * 40
    return {"Authorization": f"Bearer {token}"}


def test_command_is_forwarded_to_the_cluster(client):
    cluster = FakeCluster(
        result={"code": 200, "deduplicated": False, "applied_index": 1, "result": {}}
    )
    api.set_cluster(cluster)

    response = client.post("/v1/commands", json=command(), headers=headers())
    assert response.status_code == 200
    assert response.json()["code"] == 200
    assert cluster.proposed == [command()]


def test_command_without_leader_returns_503(client):
    api.set_cluster(FakeCluster(error=NoLeaderError("raft: no leader available")))

    response = client.post("/v1/commands", json=command(), headers=headers())
    assert response.status_code == 503
    assert "no leader" in response.json()["detail"]["reason"]


def test_readiness_requires_a_leader_and_quorum(client):
    api.set_cluster(FakeCluster())
    assert client.get("/ready").status_code == 200

    unavailable = FakeCluster()
    unavailable.status = lambda: {
        "leader_id": None,
        "alive_replicas": 2,
        "quorum_size": 3,
        "quorum_available": False,
    }
    api.set_cluster(unavailable)
    response = client.get("/ready")

    assert response.status_code == 503
    assert response.json()["detail"]["reason"] == "cluster_not_ready"


def test_command_without_quorum_returns_503(client):
    api.set_cluster(FakeCluster(result=None))

    response = client.post("/v1/commands", json=command(), headers=headers())
    assert response.status_code == 503
    assert "quorum" in response.json()["detail"]["reason"]


def test_domain_error_is_propagated(client):
    api.set_cluster(
        FakeCluster(result={"code": 409, "deduplicated": False, "result": {}})
    )

    response = client.post("/v1/commands", json=command(), headers=headers())
    assert response.status_code == 409


def test_reads_are_served_by_the_cluster(client):
    api.set_cluster(FakeCluster())

    status = client.get("/v1/status", headers=headers()).json()
    assert status["leader_id"] == "n0"
    assert status["replicas"]["n0"]["term"] == 3

    snapshot = client.get("/v1/kv/*", headers=headers()).json()
    assert snapshot["applied_index"] == 1

    event = client.get(
        "/v1/kv/alice:01J00000000000000000000001", headers=headers()
    ).json()
    assert event["found"] is True

    missing = client.get("/v1/kv/alice:missing", headers=headers()).json()
    assert missing["found"] is False

    assert client.get("/v1/availability", headers=headers()).json()["users"] == {
        "alice": "busy"
    }
    assert client.get(
        "/v1/conflicts?user=alice&start=2026-11-21T09:00:00Z&end=2026-11-21T10:00:00Z",
        headers=headers(),
    ).json()["conflicts"] == []
    assert client.get("/v1/sync", headers=headers()).json()["next"] == 0


def test_legacy_path_serves_without_cluster(client):
    response = client.post("/v1/commands", json=command(), headers=headers())
    assert response.status_code == 200
    assert client.get("/v1/status", headers=headers()).json()["middleware"] == "ray"


def test_private_event_details_are_hidden_from_other_users(client):
    api.set_cluster(FakeCluster())

    hidden = client.get(
        "/v1/kv/alice:01J00000000000000000000001",
        headers=headers("bob"),
    ).json()
    public = client.get(
        "/v1/kv/bob:01J00000000000000000000002",
        headers=headers("alice"),
    ).json()

    assert hidden == {
        "key": "alice:01J00000000000000000000001",
        "value": None,
        "found": False,
    }
    assert public["found"] is True
    assert public["value"]["payload"]["title"] == "Público"


def test_snapshot_contains_only_private_events_the_user_can_see(client):
    api.set_cluster(FakeCluster())

    response = client.get("/v1/kv/*", headers=headers("bob"))

    assert set(response.json()["events"]) == {
        "bob:01J00000000000000000000002"
    }


def test_conflicts_cannot_be_queried_for_another_user(client):
    api.set_cluster(FakeCluster())

    response = client.get(
        "/v1/conflicts?user=alice&start=2026-11-21T09:00:00Z&end=2026-11-21T10:00:00Z",
        headers=headers("bob"),
    )

    assert response.status_code == 403
