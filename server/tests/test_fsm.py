import copy

import pytest

from server.ray_app.fsm import CalendarFSM


def request(msg_id="01J00000000000000000000000", sequence=1, sender="alice", uid=None):
    return {
        "deployment_id": "dep1",
        "group_id": "team",
        "msg_id": msg_id,
        "type": "REQUEST",
        "uid": uid or f"{sender}:01J00000000000000000000001",
        "sender_id": sender,
        "key_id": "key1",
        "sequence": sequence,
        "seen_sequence": 0,
        "client_sent_at": "2026-11-20T00:00:00Z",
        "payload": {"title": "Diseño"},
    }


def test_request_and_snapshot_replay():
    fsm = CalendarFSM()
    first = fsm.apply(request())
    assert first["code"] == 200
    snapshot = fsm.snapshot()
    replayed = CalendarFSM.from_snapshot(snapshot)
    assert replayed.snapshot() == snapshot

    second = replayed.apply(request())
    assert second["deduplicated"] is True
    assert replayed.applied_index == 1


def test_unordered_creation_does_not_consume_index():
    fsm = CalendarFSM()
    result = fsm.apply(request(sequence=2))
    assert result["code"] == 409
    assert fsm.applied_index == 0
    assert fsm.snapshot()["events"] == {}


def test_group_replies_confirm_after_all_attendees_accept():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "title": "Planificación",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob", "carol"],
    }
    assert fsm.apply(proposal)["result"]["state"] == "PROPOSED"

    for sender, msg_id in (("bob", "01J00000000000000000000005"), ("carol", "01J00000000000000000000006")):
        reply = request(msg_id=msg_id, sender=sender, uid=proposal["uid"])
        reply["type"] = "REPLY"
        reply["payload"] = {"partstat": "ACCEPTED"}
        assert fsm.apply(reply)["code"] == 200

    assert fsm.events[proposal["uid"]]["state"] == "CONFIRMED"


def test_conflicting_personal_event_is_rejected():
    fsm = CalendarFSM()
    first = request()
    first["payload"] = {
        "kind": "personal",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
    }
    assert fsm.apply(first)["code"] == 200

    second = request(msg_id="01J00000000000000000000007", uid="alice:01J00000000000000000000008")
    second["payload"] = {
        "kind": "personal",
        "start": "2026-11-21T10:30:00Z",
        "end": "2026-11-21T11:30:00Z",
    }
    assert fsm.apply(second)["result"]["reason"] == "organizer_schedule_conflict"


def test_sync_and_availability_are_cursor_based_and_private():
    fsm = CalendarFSM()
    update = request()
    update["type"] = "BUSY_UPDATE"
    update["payload"] = {
        "pub_seq": 1,
        "windows": [{"start": "2026-11-21T10:00:00Z", "end": "2026-11-21T11:00:00Z", "visibility": "details"}],
    }
    assert fsm.apply(update)["code"] == 200

    availability = fsm.availability(["alice"], "2026-11-21T09:00:00Z", "2026-11-21T12:00:00Z")
    assert availability["users"]["alice"] == [
        {"start": "2026-11-21T10:00:00Z", "end": "2026-11-21T11:00:00Z", "visibility": "busy"}
    ]
    assert fsm.sync(0)["entries"][0]["index"] == 1
