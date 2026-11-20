from fastapi.testclient import TestClient

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


def test_status_and_command():
    client = TestClient(app)
    assert client.get("/health").status_code == 200
    response = client.post("/v1/commands", json=command("01J00000000000000000000002"))
    assert response.status_code == 200
    status = client.get("/v1/status").json()
    assert status["middleware"] == "ray"
