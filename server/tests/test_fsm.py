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


def test_majority_confirmation_uses_the_selected_rule():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob", "carol", "dave"],
        "confirmation_rule": "MAJORITY",
    }
    fsm.apply(proposal)

    for sender, msg_id in (
        ("bob", "01J00000000000000000000031"),
        ("carol", "01J00000000000000000000032"),
    ):
        reply = request(msg_id=msg_id, sender=sender, uid=proposal["uid"])
        reply["type"] = "REPLY"
        reply["payload"] = {"partstat": "ACCEPTED"}
        fsm.apply(reply)

    assert fsm.events[proposal["uid"]]["state"] == "CONFIRMED"


def test_personal_privacy_controls_freebusy_without_hiding_organizer_conflicts():
    fsm = CalendarFSM()
    event = request()
    event["payload"] = {
        "kind": "personal",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "availability": "busy",
        "hide_from_freebusy": True,
    }
    fsm.apply(event)

    result = fsm.availability(
        ["alice", "bob"], "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z", "alice"
    )
    assert result["users"] == {"alice": "busy", "bob": "available"}
    external_result = fsm.availability(
        ["alice"], "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z", "bob"
    )
    assert external_result["users"] == {"alice": "available"}
    assert fsm.conflicts(
        "alice", "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z"
    )["conflicts"]


def test_organizer_can_close_and_reopen_poll_and_counters_sync():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob"],
        "confirmation_rule": "ORGANIZER",
    }
    fsm.apply(proposal)

    counter = request(
        msg_id="01J00000000000000000000033",
        sender="bob",
        uid=proposal["uid"],
    )
    counter["type"] = "COUNTER"
    counter["payload"] = {
        "proposed_start": "2026-11-21T12:00:00Z",
        "proposed_end": "2026-11-21T13:00:00Z",
    }
    assert fsm.apply(counter)["code"] == 200
    assert fsm.sync_for("alice")["snapshot"]["counters"][proposal["uid"]]["bob"] == counter["payload"]

    close = request(msg_id="01J00000000000000000000034", uid=proposal["uid"])
    close["type"] = "CLOSE_POLL"
    close["payload"] = {}
    assert fsm.apply(close)["result"]["state"] == "CONFIRMED"

    reopen = request(msg_id="01J00000000000000000000035", uid=proposal["uid"])
    reopen["type"] = "REOPEN"
    reopen["payload"] = {"reask": True}
    assert fsm.apply(reopen)["result"]["state"] == "PROPOSED"
    assert fsm.attendance[proposal["uid"]] == {}


def test_calendar_share_controls_private_visibility_and_edit_access():
    fsm = CalendarFSM()
    share = request()
    share["type"] = "SHARE_CALENDAR"
    share["uid"] = "alice:calendar:personal"
    share["payload"] = {
        "calendar": "personal",
        "users": ["bob"],
        "permission": "details",
    }
    assert fsm.apply(share)["code"] == 200

    event = request(msg_id="01J00000000000000000000036")
    event["payload"] = {
        "kind": "personal",
        "title": "Privado",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "visibility": "private",
    }
    fsm.apply(event)
    bob_snapshot = fsm.sync_for("bob")["snapshot"]
    assert event["uid"] in bob_snapshot["events"]
    assert bob_snapshot["calendar_shares"][0]["users"] == ["bob"]

    share["msg_id"] = "01J00000000000000000000037"
    share["payload"] = {
        "calendar": "personal",
        "users": ["bob"],
        "permission": "edit",
    }
    fsm.apply(share)
    update = request(
        msg_id="01J00000000000000000000038",
        sender="bob",
        sequence=2,
        uid=event["uid"],
    )
    update["payload"] = {
        "kind": "personal",
        "title": "Actualizado por Bob",
        "start": "2026-11-21T12:00:00Z",
        "end": "2026-11-21T13:00:00Z",
        "visibility": "private",
    }
    assert fsm.apply(update)["code"] == 200
    assert fsm.events[event["uid"]]["payload"]["title"] == "Actualizado por Bob"

    cancel = request(
        msg_id="01J00000000000000000000039",
        sender="bob",
        uid=event["uid"],
    )
    cancel["type"] = "CANCEL"
    assert fsm.apply(cancel)["code"] == 200
    assert fsm.events[event["uid"]]["state"] == "CANCELLED"


def test_personal_event_of_another_user_does_not_block_or_leak():
    fsm = CalendarFSM()
    alice_event = request()
    alice_event["payload"] = {
        "kind": "personal",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "availability": "busy",
        "hide_from_freebusy": False,
    }
    assert fsm.apply(alice_event)["code"] == 200

    # Alice sigue apareciendo como ocupada en las consultas de disponibilidad.
    assert fsm.availability(
        ["alice"], "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z", "bob"
    )["users"] == {"alice": "busy"}

    # ...pero el evento personal de Alice no ocupa el calendario de Bob.
    assert fsm.availability(
        ["bob"], "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z", "bob"
    )["users"] == {"bob": "available"}

    # ...y su calendario personal no se filtra en los conflictos de Bob.
    assert fsm.conflicts(
        "bob", "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z"
    )["conflicts"] == []

    # Bob puede crear su propio evento solapado.
    bob_event = request(msg_id="01J00000000000000000000041", sender="bob")
    bob_event["payload"] = {
        "kind": "personal",
        "start": "2026-11-21T10:30:00Z",
        "end": "2026-11-21T11:30:00Z",
    }
    assert fsm.apply(bob_event)["code"] == 200

    # Cada usuario solo ve los conflictos de su propio calendario.
    bob_conflicts = fsm.conflicts(
        "bob", "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z"
    )["conflicts"]
    assert [item["uid"] for item in bob_conflicts] == [bob_event["uid"]]
    alice_conflicts = fsm.conflicts(
        "alice", "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z"
    )["conflicts"]
    assert [item["uid"] for item in alice_conflicts] == [alice_event["uid"]]


def test_shared_meeting_still_conflicts_for_its_participants():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob"],
    }
    assert fsm.apply(proposal)["code"] == 200

    reply = request(
        msg_id="01J00000000000000000000051", sender="bob", uid=proposal["uid"]
    )
    reply["type"] = "REPLY"
    reply["payload"] = {"partstat": "ACCEPTED"}
    assert fsm.apply(reply)["code"] == 200
    assert fsm.events[proposal["uid"]]["state"] == "CONFIRMED"

    # El evento compartido cuenta como conflicto para Bob...
    conflicts = fsm.conflicts(
        "bob", "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z"
    )["conflicts"]
    assert [item["uid"] for item in conflicts] == [proposal["uid"]]

    # ...y Bob no puede solaparlo con un evento propio.
    bob_event = request(msg_id="01J00000000000000000000052", sender="bob")
    bob_event["payload"] = {
        "kind": "personal",
        "start": "2026-11-21T10:30:00Z",
        "end": "2026-11-21T11:30:00Z",
    }
    result = fsm.apply(bob_event)

    assert result["code"] == 409
    assert result["result"]["reason"] == "organizer_schedule_conflict"


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
        "windows": [
            {
                "start": "2026-11-21T10:00:00Z",
                "end": "2026-11-21T11:00:00Z",
                "availability": "busy",
            }
        ],
    }
    assert fsm.apply(update)["code"] == 200

    availability = fsm.availability(["alice"], "2026-11-21T09:00:00Z", "2026-11-21T12:00:00Z")
    assert availability["users"] == {"alice": "busy"}
    page = fsm.sync(0)
    assert page["entries"] == []
    assert page["next"] == 1


def test_sync_redacts_busy_updates_but_advances_past_their_log_indexes():
    fsm = CalendarFSM()
    update = request()
    update["type"] = "BUSY_UPDATE"
    update["payload"] = {
        "pub_seq": 1,
        "windows": [
            {
                "start": "2026-11-21T09:00:00Z",
                "end": "2026-11-21T10:00:00Z",
                "availability": "busy",
            }
        ],
    }
    fsm.apply(update)

    event = request(msg_id="01J00000000000000000000009")
    fsm.apply(event)

    page = fsm.sync(0, limit=1)
    assert page == {"entries": [], "next": 1, "as_of": 2}
    page = fsm.sync(page["next"], limit=1)
    assert [entry["index"] for entry in page["entries"]] == [2]


def test_equal_busy_publication_sequence_is_ignored():
    fsm = CalendarFSM()
    first = request()
    first["type"] = "BUSY_UPDATE"
    first["payload"] = {"pub_seq": 1, "windows": []}
    fsm.apply(first)

    duplicate_sequence = request(msg_id="01J00000000000000000000010")
    duplicate_sequence["type"] = "BUSY_UPDATE"
    duplicate_sequence["payload"] = {
        "pub_seq": 1,
        "windows": [
            {
                "start": "2026-11-21T09:00:00Z",
                "end": "2026-11-21T10:00:00Z",
                "availability": "busy",
            }
        ],
    }
    result = fsm.apply(duplicate_sequence)

    assert result["result"]["stale"] is True
    assert fsm.busy["alice"]["windows"] == []


def test_cancelled_event_does_not_block_a_new_event():
    fsm = CalendarFSM()
    first = request()
    first["payload"] = {
        "kind": "personal",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
    }
    fsm.apply(first)

    cancelled = request(msg_id="01J00000000000000000000011")
    cancelled["type"] = "CANCEL"
    cancelled["uid"] = first["uid"]
    cancelled["sequence"] = 2
    assert fsm.apply(cancelled)["code"] == 200

    second = request(msg_id="01J00000000000000000000012")
    second["uid"] = "alice:01J00000000000000000000013"
    second["payload"] = {
        "kind": "personal",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
    }
    assert fsm.apply(second)["code"] == 200


def test_unsupported_commands_are_not_reported_as_success():
    fsm = CalendarFSM()
    command = request()
    command["type"] = "DECLINECOUNTER"

    result = fsm.apply(command)

    assert result["code"] == 422
    assert result["result"]["reason"] == "unsupported_command"
    assert fsm.applied_index == 0


def test_busy_and_available_windows_have_only_two_statuses_and_busy_wins():
    fsm = CalendarFSM()
    update = request()
    update["type"] = "BUSY_UPDATE"
    update["payload"] = {
        "pub_seq": 1,
        "windows": [
            {
                "start": "2026-11-21T09:00:00-05:00",
                "end": "2026-11-21T10:00:00-05:00",
                "availability": "available",
            },
            {
                "start": "2026-11-21T14:30:00Z",
                "end": "2026-11-21T15:00:00Z",
                "availability": "busy",
            },
        ],
    }

    assert fsm.apply(update)["code"] == 200
    assert fsm.availability(
        ["alice"], "2026-11-21T14:45:00Z", "2026-11-21T15:30:00Z"
    )["users"]["alice"] == "busy"
    assert fsm.availability(
        ["alice"], "2026-11-21T15:00:00Z", "2026-11-21T15:30:00Z"
    )["users"]["alice"] == "available"


@pytest.mark.parametrize(
    "window",
    [
        {
            "start": "2026-11-21T10:00:00",
            "end": "2026-11-21T11:00:00Z",
            "availability": "busy",
        },
        {
            "start": "2026-11-21T10:00:00Z",
            "end": "2026-11-21T11:00:00Z",
            "availability": "tentative",
        },
        {
            "start": "2026-11-21T11:00:00Z",
            "end": "2026-11-21T10:00:00Z",
            "availability": "busy",
        },
    ],
)
def test_invalid_availability_window_is_rejected_without_consuming_log(window):
    fsm = CalendarFSM()
    update = request()
    update["type"] = "BUSY_UPDATE"
    update["payload"] = {"pub_seq": 1, "windows": [window]}

    result = fsm.apply(update)

    assert result["code"] == 422
    assert result["result"]["reason"] == "invalid_window"
    assert fsm.applied_index == 0


def test_private_group_is_visible_only_to_organizer_and_attendees_in_sync():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "title": "Privado",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob"],
        "visibility": "private",
    }
    fsm.apply(proposal)

    assert fsm.sync_for("alice")["entries"]
    assert fsm.sync_for("bob")["entries"]
    assert fsm.sync_for("carol")["entries"] == []
    assert fsm.sync_for("carol")["next"] == 1
    assert fsm.sync_for("carol")["snapshot"]["events"] == {}


def test_public_event_is_visible_to_other_users_in_sync():
    fsm = CalendarFSM()
    event = request()
    event["payload"] = {
        "title": "Public",
        "visibility": "public",
    }

    fsm.apply(event)

    assert fsm.sync_for("bob")["entries"][0]["command"]["uid"] == event["uid"]


@pytest.mark.parametrize(
    "payload",
    [
        {"visibility": "restricted"},
        {"start": "2026-11-21T10:00:00", "end": "2026-11-21T11:00:00Z"},
        {"start": "2026-11-21T11:00:00Z", "end": "2026-11-21T10:00:00Z"},
        {"start": "2026-11-21T10:00:00Z"},
    ],
)
def test_invalid_event_visibility_or_time_window_is_rejected(payload):
    fsm = CalendarFSM()
    event = request()
    event["payload"] = payload

    result = fsm.apply(event)

    assert result["code"] == 422
    assert fsm.applied_index == 0
    assert fsm.events == {}


def test_decline_rejects_group_and_stops_counting_it_as_busy():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob", "carol"],
    }
    fsm.apply(proposal)

    reply = request(
        msg_id="01J00000000000000000000020",
        sender="bob",
        uid=proposal["uid"],
    )
    reply["type"] = "REPLY"
    reply["payload"] = {"partstat": "DECLINED"}
    result = fsm.apply(reply)

    assert result["code"] == 200
    assert result["result"]["partstat"] == "DECLINED"
    assert fsm.events[proposal["uid"]]["state"] == "NOT_CONFIRMED"
    assert fsm.availability(
        ["bob"], "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z"
    )["users"]["bob"] == "available"


def test_attendee_cannot_accept_when_they_have_a_busy_window():
    fsm = CalendarFSM()
    busy = request(sender="bob")
    busy["type"] = "BUSY_UPDATE"
    busy["payload"] = {
        "pub_seq": 1,
        "windows": [
            {
                "start": "2026-11-21T10:00:00Z",
                "end": "2026-11-21T11:00:00Z",
                "availability": "busy",
            }
        ],
    }
    fsm.apply(busy)

    proposal = request(
        msg_id="01J00000000000000000000021",
        uid="alice:01J00000000000000000000022",
    )
    proposal["payload"] = {
        "kind": "group",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob"],
    }
    fsm.apply(proposal)
    reply = request(
        msg_id="01J00000000000000000000023",
        sender="bob",
        uid=proposal["uid"],
    )
    reply["type"] = "REPLY"
    reply["payload"] = {"partstat": "ACCEPTED"}

    result = fsm.apply(reply)

    assert result["code"] == 409
    assert result["result"]["reason"] == "attendee_schedule_conflict"
    assert fsm.attendance[proposal["uid"]] == {}


def test_event_update_reopens_group_and_resets_previous_acceptances():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob"],
    }
    fsm.apply(proposal)

    reply = request(
        msg_id="01J00000000000000000000024",
        sender="bob",
        uid=proposal["uid"],
    )
    reply["type"] = "REPLY"
    reply["payload"] = {"partstat": "ACCEPTED"}
    fsm.apply(reply)
    assert fsm.events[proposal["uid"]]["state"] == "CONFIRMED"

    update = request(
        msg_id="01J00000000000000000000025",
        sequence=2,
        uid=proposal["uid"],
    )
    update["payload"] = {
        "start": "2026-11-21T12:00:00Z",
        "end": "2026-11-21T13:00:00Z",
        "participants": ["bob"],
        "visibility": "public",
    }

    result = fsm.apply(update)

    assert result["code"] == 200
    event = fsm.events[proposal["uid"]]
    assert event["state"] == "PROPOSED"
    assert event["visibility"] == "public"
    assert fsm.attendance[proposal["uid"]] == {}
