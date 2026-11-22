import json

from fastapi.testclient import TestClient
import pytest

from server.ray_app.api import app


def command(msg_id):
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
        "client_sent_at": "2026-11-20T00:00:00Z",
        "payload": {"title": "Prueba"},
    }


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv(
        "SDCAL_API_TOKENS",
        json.dumps({"alice": "a" * 40}),
    )
    return TestClient(app)


def test_status_and_command(client):
    headers = {"Authorization": "Bearer " + "a" * 40}
    assert client.get("/health").status_code == 200
    assert client.get("/ready").status_code == 200
    response = client.post(
        "/v1/commands",
        json=command("01J00000000000000000000002"),
        headers=headers,
    )
    assert response.status_code == 200
    status = client.get("/v1/status", headers=headers).json()
    assert status["middleware"] == "ray"


def test_data_routes_require_a_bearer_token(client):
    response = client.post(
        "/v1/commands",
        json=command("01J00000000000000000000003"),
    )

    assert response.status_code == 401


def test_command_sender_must_match_authenticated_identity(client):
    body = command("01J00000000000000000000004")
    body["sender_id"] = "bob"

    response = client.post(
        "/v1/commands",
        json=body,
        headers={"Authorization": "Bearer " + "a" * 40},
    )

    assert response.status_code == 403
    assert response.json()["detail"]["reason"] == "sender_identity_mismatch"
