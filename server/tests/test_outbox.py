import pytest

from client.outbox import Outbox, OutboxDeliveryError


def command(msg_id="01J00000000000000000000000"):
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
        "payload": {"title": "Reunión"},
    }


def test_outbox_retries_same_complete_command_after_a_disconnect(tmp_path):
    outbox = Outbox(str(tmp_path / "outbox.db"))
    pending = command()
    outbox.enqueue(pending)

    def disconnected(_command):
        raise TimeoutError("server unavailable")

    with pytest.raises(TimeoutError, match="server unavailable"):
        outbox.send_pending(disconnected)
    assert outbox.pending()[0] == {
        "msg_id": pending["msg_id"],
        "uid": pending["uid"],
        "sequence": pending["sequence"],
        "command": pending,
        "attempts": 1,
    }

    sent = []
    assert outbox.send_pending(
        lambda value: sent.append(value)
        or {"code": 200, "deduplicated": False}
    ) == 1
    assert sent == [pending]
    assert outbox.pending() == []
    outbox.close()


def test_outbox_rejects_reuse_of_a_msg_id_for_different_content(tmp_path):
    outbox = Outbox(str(tmp_path / "outbox.db"))
    outbox.enqueue(command())

    altered = command()
    altered["payload"] = {"title": "Otro contenido"}
    with pytest.raises(ValueError, match="already used"):
        outbox.enqueue(altered)
    outbox.close()


def test_conflicting_command_is_kept_for_explicit_reconciliation(tmp_path):
    outbox = Outbox(str(tmp_path / "outbox.db"))
    pending = command()
    outbox.enqueue(pending)
    response = {"code": 409, "result": {"reason": "sequence_gap"}}

    with pytest.raises(OutboxDeliveryError):
        outbox.send_pending(lambda _command: response)

    assert outbox.pending() == []
    assert outbox.conflicts()[0] == {
        "msg_id": pending["msg_id"],
        "uid": pending["uid"],
        "sequence": pending["sequence"],
        "command": pending,
        "attempts": 1,
        "error": {"command_error": response},
    }
    outbox.close()


def test_reconcile_persists_entries_and_advances_over_filtered_records(tmp_path):
    outbox = Outbox(str(tmp_path / "outbox.db"))
    first = {"index": 2, "command": {"type": "REQUEST"}}
    second = {"index": 3, "command": {"type": "REPLY"}}

    assert outbox.reconcile([first], next_cursor=2) == 2
    assert outbox.reconcile([second], next_cursor=4) == 4
    assert outbox.cursor() == 4
    outbox.close()


def test_reconcile_is_idempotent_for_a_replayed_page(tmp_path):
    outbox = Outbox(str(tmp_path / "outbox.db"))
    entry = {"index": 1, "command": {"type": "REQUEST"}}

    assert outbox.reconcile([entry], next_cursor=2) == 2
    assert outbox.reconcile([entry], next_cursor=2) == 2
    outbox.close()


def test_reconcile_rolls_back_entry_when_cursor_is_invalid(tmp_path):
    outbox = Outbox(str(tmp_path / "outbox.db"))
    entry = {"index": 2, "command": {"type": "REQUEST"}}

    with pytest.raises(ValueError, match="precedes returned entries"):
        outbox.reconcile([entry], next_cursor=1)

    assert outbox.cursor() == 0
    assert outbox.synced_snapshot() == {
        "events": {},
        "attendance": {},
        "applied_index": 0,
    }
    outbox.close()


def test_reconcile_replaces_local_projection_with_authorized_snapshot(tmp_path):
    outbox = Outbox(str(tmp_path / "outbox.db"))
    private = {
        "uid": "alice:event-1",
        "visibility": "private",
        "state": "CONFIRMED",
    }
    public = {
        "uid": "bob:event-2",
        "visibility": "public",
        "state": "CONFIRMED",
    }

    outbox.reconcile(
        [],
        next_cursor=1,
        snapshot={
            "events": {private["uid"]: private},
            "attendance": {},
            "applied_index": 1,
        },
    )
    outbox.reconcile(
        [],
        next_cursor=2,
        snapshot={
            "events": {public["uid"]: public},
            "attendance": {},
            "applied_index": 2,
        },
    )

    assert outbox.synced_snapshot() == {
        "events": {public["uid"]: public},
        "attendance": {public["uid"]: {}},
        "applied_index": 2,
    }
    outbox.close()


def test_reconcile_rejects_noncontiguous_entries_without_server_cursor(tmp_path):
    outbox = Outbox(str(tmp_path / "outbox.db"))
    with pytest.raises(ValueError, match="not contiguous"):
        outbox.reconcile([{"index": 2}])
    outbox.close()
