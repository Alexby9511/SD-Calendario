import json

from fastapi.testclient import TestClient
import pytest

from server.ray_app import api
from server.ray_app.api import app
from server.ray_app.coordinator import Coordinator
from server.ray_app.auth import TokenRegistry


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


def personal_command(msg_id, sender, uid, start, end):
    body = command(msg_id)
    body["sender_id"] = sender
    body["uid"] = uid
    body["payload"] = {
        "kind": "personal",
        "title": "Cita",
        "start": start,
        "end": end,
    }
    return body


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("SDCAL_AUTH_SECRET", "test-signing-secret-that-is-at-least-32-bytes")
    monkeypatch.setenv(
        "SDCAL_API_TOKENS",
        json.dumps({"alice": "a" * 40, "bob": "b" * 40}),
    )
    monkeypatch.setattr(api, "local_coordinator", Coordinator())
    api.set_cluster(None)
    return TestClient(app)


def headers(user="alice"):
    token = "a" * 40 if user == "alice" else "b" * 40
    return {"Authorization": f"Bearer {token}"}


def test_status_and_command(client):
    assert client.get("/health").status_code == 200
    assert client.get("/ready").status_code == 200
    response = client.post(
        "/v1/commands",
        json=command("01J00000000000000000000002"),
        headers=headers(),
    )
    assert response.status_code == 200
    status = client.get("/v1/status", headers=headers()).json()
    assert status["middleware"] == "local"


def test_sync_rejects_limits_over_one_thousand(client):
    response = client.get("/v1/sync?limit=1001", headers=headers())

    assert response.status_code == 422
    assert response.json()["detail"]["reason"] == "invalid_sync_limit"


def test_token_registry_reads_current_environment(monkeypatch):
    first_token = "a" * 40
    second_token = "b" * 40
    monkeypatch.setenv("SDCAL_API_TOKENS", json.dumps({"alice": first_token}))
    assert TokenRegistry.from_environment().authenticate(first_token) == "alice"

    monkeypatch.setenv("SDCAL_API_TOKENS", json.dumps({"bob": second_token}))
    assert TokenRegistry.from_environment().authenticate(second_token) == "bob"


def test_registration_login_and_calendar_data_use_the_authenticated_account(client):
    registered = client.post(
        "/v1/auth/register",
        json={"user_id": "New_User", "password": "secure password"},
    )
    assert registered.status_code == 201
    credentials = registered.json()
    assert credentials["user_id"] == "new_user"
    token_headers = {"Authorization": f"Bearer {credentials['access_token']}"}

    assert client.get("/v1/auth/me", headers=token_headers).json() == {
        "user_id": "new_user"
    }
    created = client.post(
        "/v1/commands",
        json=personal_command(
            "01J00000000000000000000021",
            "new_user",
            "new_user:01J00000000000000000000022",
            "2026-11-21T10:00:00Z",
            "2026-11-21T11:00:00Z",
        ),
        headers=token_headers,
    )
    assert created.status_code == 200
    assert client.get("/v1/kv/*", headers=token_headers).json().get("accounts") is None

    duplicate = client.post(
        "/v1/auth/register",
        json={"user_id": "NEW_USER", "password": "different password"},
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["detail"]["reason"] == "user_id_taken"

    assert client.post(
        "/v1/auth/login",
        json={"user_id": "new_user", "password": "wrong password"},
    ).status_code == 401
    signed_in = client.post(
        "/v1/auth/login",
        json={"user_id": "NEW_USER", "password": "secure password"},
    )
    assert signed_in.status_code == 200
    assert signed_in.json()["user_id"] == "new_user"


def test_registration_validates_identifier_and_password(client):
    invalid_id = client.post(
        "/v1/auth/register",
        json={"user_id": "a!", "password": "secure password"},
    )
    assert invalid_id.status_code == 422
    assert invalid_id.json()["detail"]["reason"] == "invalid_user_id"

    weak_password = client.post(
        "/v1/auth/register",
        json={"user_id": "valid_user", "password": "short"},
    )
    assert weak_password.status_code == 422
    assert weak_password.json()["detail"]["reason"] == "weak_password"


def test_calendar_commands_cannot_submit_internal_account_operations(client):
    response = client.post(
        "/v1/commands",
        json={"internal_type": "REGISTER_ACCOUNT", "user_id": "another_user"},
        headers=headers(),
    )

    assert response.status_code == 422
    assert response.json()["detail"]["reason"] == "unsupported_command"


def test_calendar_commands_cannot_submit_system_evaluation(client):
    body = command("01J00000000000000000000031")
    body["type"] = "EVALUATE"
    body["uid"] = "system:calendar"
    body["payload"] = {"now": "2030-01-01T00:00:00Z"}

    response = client.post("/v1/commands", json=body, headers=headers())

    assert response.status_code == 422
    assert response.json()["detail"]["reason"] == "unsupported_command"


def test_reserved_system_identity_cannot_register(client):
    response = client.post(
        "/v1/auth/register",
        json={"user_id": "System", "password": "secure password"},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["reason"] == "invalid_user_id"


def test_local_availability_timeout_returns_503(client, monkeypatch):
    def timeout(*args, **kwargs):
        raise TimeoutError("query timed out")

    monkeypatch.setattr(api.local_coordinator, "availability", timeout)

    response = client.get("/v1/availability", headers=headers())

    assert response.status_code == 503
    assert response.json()["detail"]["reason"] == "query_timeout"


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
        headers=headers("alice"),
    )

    assert response.status_code == 403
    assert response.json()["detail"]["reason"] == "sender_identity_mismatch"


def test_overflowing_sequence_is_rejected_instead_of_crashing_the_service(client):
    body = (
        '{"deployment_id":"dep1","group_id":"team",'
        '"msg_id":"01J00000000000000000000005","type":"REQUEST",'
        '"uid":"alice:01J00000000000000000000006","sender_id":"alice",'
        '"key_id":"key1","sequence":1e999,"seen_sequence":0,'
        '"client_sent_at":"2026-11-20T00:00:00Z","payload":{"title":"Prueba"}}'
    )

    response = client.post(
        "/v1/commands",
        content=body,
        headers={**headers(), "Content-Type": "application/json"},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == 422

    # El servicio sigue admitiendo comandos válidos.
    ok = client.post(
        "/v1/commands",
        json=command("01J00000000000000000000007"),
        headers=headers(),
    )
    assert ok.status_code == 200


def test_personal_event_of_another_user_is_not_reported_as_a_conflict(client):
    created = client.post(
        "/v1/commands",
        json=personal_command(
            "01J00000000000000000000011",
            "alice",
            "alice:01J00000000000000000000012",
            "2026-11-21T10:00:00Z",
            "2026-11-21T11:00:00Z",
        ),
        headers=headers("alice"),
    )
    assert created.status_code == 200

    conflicts = client.get(
        "/v1/conflicts?user=bob"
        "&start=2026-11-21T10:00:00Z&end=2026-11-21T11:00:00Z",
        headers=headers("bob"),
    )
    assert conflicts.status_code == 200
    assert conflicts.json()["conflicts"] == []

    # Bob puede crear su propio evento solapado.
    bob_event = client.post(
        "/v1/commands",
        json=personal_command(
            "01J00000000000000000000013",
            "bob",
            "bob:01J00000000000000000000014",
            "2026-11-21T10:30:00Z",
            "2026-11-21T11:30:00Z",
        ),
        headers=headers("bob"),
    )
    assert bob_event.status_code == 200
