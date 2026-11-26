import copy

import pytest

from server.ray_app import fsm as fsm_module
from server.ray_app.fsm import CalendarFSM
from server.ray_app.state import CalendarState


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


def test_read_snapshot_omits_sync_history():
    fsm = CalendarFSM()
    assert fsm.apply(request())["code"] == 200

    snapshot = fsm.read_snapshot()

    assert "events" in snapshot
    assert "log" not in snapshot
    assert "seen" not in snapshot


def test_registered_accounts_are_replicated_in_snapshots():
    fsm = CalendarFSM()
    command = {
        "internal_type": "REGISTER_ACCOUNT",
        "user_id": "alice",
        "password_salt": "a" * 32,
        "password_hash": "b" * 64,
    }

    assert fsm.apply(command)["code"] == 200
    replayed = CalendarFSM.from_snapshot(fsm.snapshot())

    assert replayed.accounts == {
        "alice": {"password_salt": "a" * 32, "password_hash": "b" * 64}
    }
    assert replayed.apply({**command, "password_hash": "c" * 64})["code"] == 409
    assert replayed.applied_index == fsm.applied_index


def test_lone_surrogate_command_is_rejected_without_touching_the_log():
    state = CalendarState()
    command = request()
    command["group_id"] = "\ud800"

    result = state.apply(command)

    assert result["code"] == 422
    assert state.fsm.applied_index == 0
    assert state.fsm.log == []


def test_system_identity_cannot_be_registered():
    fsm = CalendarFSM()
    command = {
        "internal_type": "REGISTER_ACCOUNT",
        "user_id": "system",
        "password_salt": "a" * 32,
        "password_hash": "b" * 64,
    }

    result = fsm.apply(command)

    assert result["code"] == 422
    assert result["result"]["reason"] == "invalid_account"
    assert "system" not in fsm.accounts
    assert fsm.applied_index == 0


def test_unordered_creation_does_not_consume_index():
    fsm = CalendarFSM()
    result = fsm.apply(request(sequence=2))
    assert result["code"] == 409
    assert fsm.applied_index == 0
    assert fsm.snapshot()["events"] == {}


def _event_and_mutator(command_type):
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
    if command_type == "REOPEN":
        fsm.events[proposal["uid"]]["state"] = "CONFIRMED"
    command = request(
        msg_id="01J00000000000000000000042",
        sender="bob" if command_type == "COUNTER" else "alice",
        uid=proposal["uid"],
        sequence=1,
    )
    command["type"] = command_type
    command["payload"] = (
        {
            "proposed_start": "2026-11-21T12:00:00Z",
            "proposed_end": "2026-11-21T13:00:00Z",
        }
        if command_type == "COUNTER"
        else {"reask": True}
        if command_type == "REOPEN"
        else {}
    )
    return fsm, proposal, command


@pytest.mark.parametrize("command_type", ["CANCEL", "CLOSE_POLL", "REOPEN", "COUNTER"])
def test_mutating_commands_reject_stale_sequences(command_type):
    fsm, proposal, command = _event_and_mutator(command_type)

    result = fsm.apply(command)

    assert result["code"] == 409
    assert result["result"]["reason"] == "stale_sequence"
    assert fsm.events[proposal["uid"]]["sequence"] == 1
    assert fsm.applied_index == 1
    assert proposal["uid"] not in fsm.counters


@pytest.mark.parametrize("command_type", ["CANCEL", "CLOSE_POLL", "REOPEN", "COUNTER"])
def test_mutating_commands_advance_the_event_sequence(command_type):
    fsm, proposal, command = _event_and_mutator(command_type)
    if command_type == "REOPEN":
        fsm.events[proposal["uid"]]["state"] = "CONFIRMED"
    command["sequence"] = 2

    result = fsm.apply(command)

    assert result["code"] == 200
    assert fsm.events[proposal["uid"]]["sequence"] == 2


def test_stale_request_update_is_an_explicit_conflict():
    fsm = CalendarFSM()
    original = request()
    assert fsm.apply(original)["code"] == 200

    stale = request(
        msg_id="01J00000000000000000000043",
        sequence=1,
        uid=original["uid"],
    )
    result = fsm.apply(stale)

    assert result["code"] == 409
    assert result["result"]["reason"] == "stale_sequence"
    assert fsm.events[original["uid"]]["sequence"] == 1


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
        ["alice"], "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z", "alice"
    )
    assert result["users"] == {"alice": "busy"}
    # Sin un calendario compartido, otro usuario no ve ni siquiera el ocupado.
    assert fsm.availability(
        ["alice"], "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z", "bob"
    )["users"] == {}

    share = request(msg_id="01J00000000000000000000021", sequence=2)
    share["type"] = "SHARE_CALENDAR"
    share["payload"] = {
        "calendar": "personal",
        "users": ["bob"],
        "permission": "freebusy",
    }
    assert fsm.apply(share)["code"] == 200

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
    counter["sequence"] = 2
    assert fsm.apply(counter)["code"] == 200
    assert fsm.sync_for("alice")["snapshot"]["counters"][proposal["uid"]]["bob"] == counter["payload"]

    close = request(msg_id="01J00000000000000000000034", uid=proposal["uid"])
    close["type"] = "CLOSE_POLL"
    close["sequence"] = 3
    close["payload"] = {}
    assert fsm.apply(close)["result"]["state"] == "CONFIRMED"

    reopen = request(msg_id="01J00000000000000000000035", uid=proposal["uid"])
    reopen["type"] = "REOPEN"
    reopen["sequence"] = 4
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
        sequence=3,
    )
    cancel["type"] = "CANCEL"
    assert fsm.apply(cancel)["code"] == 200
    assert fsm.events[event["uid"]]["state"] == "CANCELLED"


def test_custom_calendar_share_controls_event_visibility_and_edit_access():
    fsm = CalendarFSM()
    custom_calendar_id = "custom-calendar-123"
    share = request()
    share["type"] = "SHARE_CALENDAR"
    share["uid"] = f"alice:calendar:{custom_calendar_id}"
    share["payload"] = {
        "calendar": custom_calendar_id,
        "users": ["bob"],
        "permission": "details",
    }
    assert fsm.apply(share)["code"] == 200

    shared_event = request(
        msg_id="01J00000000000000000000040",
        uid="alice:custom-event",
    )
    shared_event["payload"] = {
        "kind": "personal",
        "title": "Capa compartida",
        "calendar_id": custom_calendar_id,
    }
    assert fsm.apply(shared_event)["code"] == 200

    private_event = request(
        msg_id="01J00000000000000000000041",
        uid="alice:private-event",
    )
    private_event["payload"] = {"kind": "personal", "title": "Otra capa"}
    assert fsm.apply(private_event)["code"] == 200
    assert set(fsm.sync_for("bob")["snapshot"]["events"]) == {shared_event["uid"]}

    view_only_update = request(
        msg_id="01J00000000000000000000042",
        sender="bob",
        sequence=2,
        uid=shared_event["uid"],
    )
    view_only_update["payload"] = {
        "kind": "personal",
        "title": "No autorizado",
        "calendar_id": custom_calendar_id,
    }
    assert fsm.apply(view_only_update)["code"] == 403

    share["msg_id"] = "01J00000000000000000000043"
    share["payload"]["permission"] = "edit"
    assert fsm.apply(share)["code"] == 200

    edit = request(
        msg_id="01J00000000000000000000044",
        sender="bob",
        sequence=2,
        uid=shared_event["uid"],
    )
    edit["payload"] = {
        "kind": "personal",
        "title": "Editado en capa compartida",
        "calendar_id": custom_calendar_id,
    }
    assert fsm.apply(edit)["code"] == 200

    private_edit = request(
        msg_id="01J00000000000000000000045",
        sender="bob",
        sequence=2,
        uid=private_event["uid"],
    )
    private_edit["payload"] = {"kind": "personal", "title": "No autorizado"}
    assert fsm.apply(private_edit)["code"] == 403


def test_availability_only_reports_users_who_shared_their_calendar():
    fsm = CalendarFSM()
    for sender, msg_id in (("bob", "01J00000000000000000000051"), ("carol", "01J00000000000000000000052")):
        event = request(msg_id=msg_id, sender=sender)
        event["payload"] = {
            "kind": "personal",
            "start": "2026-11-21T10:00:00Z",
            "end": "2026-11-21T11:00:00Z",
        }
        assert fsm.apply(event)["code"] == 200

    share = request(msg_id="01J00000000000000000000053", sequence=2, sender="carol")
    share["type"] = "SHARE_CALENDAR"
    share["payload"] = {
        "calendar": "work",
        "users": ["alice"],
        "permission": "details",
    }
    assert fsm.apply(share)["code"] == 200

    result = fsm.availability(["alice", "bob", "carol"], None, None, "alice")

    assert result["users"] == {"alice": "available", "carol": "busy"}
    # Sin ``users`` explícitos tampoco se enumeran los usuarios sin compartir.
    assert fsm.availability(None, None, None, "alice")["users"] == {
        "carol": "busy",
    }


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

    # Sin calendario compartido, Bob no ve la disponibilidad de Alice.
    assert fsm.availability(
        ["alice"], "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z", "bob"
    )["users"] == {}
    # Alice sí se ve ocupada a sí misma.
    assert fsm.availability(
        ["alice"], "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z", "alice"
    )["users"] == {"alice": "busy"}

    # ...y el evento personal de Alice no ocupa el calendario de Bob.
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


def test_history_and_seen_are_bounded(monkeypatch):
    monkeypatch.setattr(fsm_module, "MAX_HISTORY_ENTRIES", 3)
    monkeypatch.setattr(fsm_module, "MAX_SEEN_PER_SENDER", 2)
    fsm = CalendarFSM()
    for n in range(5):
        event = request(
            msg_id=f"01J0000000000000000000000{n}",
            uid=f"alice:01J0000000000000000000010{n}",
        )
        assert fsm.apply(event)["code"] == 200

    assert [entry["index"] for entry in fsm.log] == [3, 4, 5]
    assert list(fsm.seen["alice"]) == [
        "01J00000000000000000000003",
        "01J00000000000000000000004",
    ]
    snapshot = fsm.snapshot()
    assert "log" not in snapshot
    assert CalendarFSM.from_snapshot(snapshot).log == []


def test_sync_for_forces_a_snapshot_when_the_cursor_fell_out_of_history(monkeypatch):
    monkeypatch.setattr(fsm_module, "MAX_HISTORY_ENTRIES", 2)
    fsm = CalendarFSM()
    for n in range(4):
        event = request(
            msg_id=f"01J0000000000000000000001{n}",
            uid=f"alice:01J0000000000000000000020{n}",
        )
        assert fsm.apply(event)["code"] == 200

    page = fsm.sync_for("alice", 0, 100, include_snapshot=False)
    assert [entry["index"] for entry in page["entries"]] == [3, 4]
    assert page["next"] == 4
    assert "snapshot" in page

    page = fsm.sync_for("alice", 2, 100, include_snapshot=False)
    assert "snapshot" not in page


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
    command["type"] = "INVALID_COMMAND"

    with pytest.raises(ValueError, match="unknown type"):
        fsm.apply(command)
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
        "description": "Notas privadas",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob"],
        "visibility": "private",
    }
    fsm.apply(proposal)

    assert fsm.sync_for("alice")["entries"]
    assert fsm.sync_for("bob")["entries"]
    assert fsm.sync_for("bob")["snapshot"]["events"][proposal["uid"]]["payload"]["description"] == "Notas privadas"
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


def test_public_event_description_is_shared_but_owner_settings_are_redacted():
    fsm = CalendarFSM()
    event = request()
    event["payload"] = {
        "kind": "personal",
        "title": "Public",
        "description": "Detalles del evento",
        "visibility": "public",
        "privacy": "public",
        "availability": "free",
        "hide_from_freebusy": True,
    }

    assert fsm.apply(event)["code"] == 200
    owner = fsm.sync_for("alice")
    viewer = fsm.sync_for("bob")
    owner_event = owner["snapshot"]["events"][event["uid"]]
    viewer_event = viewer["snapshot"]["events"][event["uid"]]

    assert owner_event["payload"]["description"] == "Detalles del evento"
    assert owner_event["payload"]["visibility"] == "public"
    assert owner_event["payload"]["availability"] == "free"
    assert owner_event["payload"]["hide_from_freebusy"] is True
    assert viewer_event["payload"]["description"] == "Detalles del evento"
    for field in ("visibility", "availability", "privacy", "hide_from_freebusy"):
        assert field not in viewer_event
        assert field not in viewer_event["payload"]
        assert field not in viewer["entries"][0]["command"]["payload"]
        assert field not in viewer["entries"][0]["event"]["payload"]


def test_shared_editor_cannot_change_owner_only_event_settings():
    fsm = CalendarFSM()
    share = request()
    share["type"] = "SHARE_CALENDAR"
    share["uid"] = "alice:calendar:personal"
    share["payload"] = {
        "calendar": "personal",
        "users": ["bob"],
        "permission": "edit",
    }
    assert fsm.apply(share)["code"] == 200

    event = request(msg_id="01J00000000000000000000037")
    event["payload"] = {
        "kind": "personal",
        "title": "Public",
        "description": "Original description",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "visibility": "public",
        "privacy": "public",
        "availability": "free",
        "hide_from_freebusy": True,
    }
    assert fsm.apply(event)["code"] == 200

    update = request(
        msg_id="01J00000000000000000000038",
        sequence=2,
        sender="bob",
        uid=event["uid"],
    )
    update["payload"] = {
        **event["payload"],
        "title": "Edited by shared user",
        "description": "Updated description",
        "visibility": "private",
        "privacy": "private",
        "availability": "busy",
        "hide_from_freebusy": False,
    }
    assert fsm.apply(update)["code"] == 200

    stored = fsm.events[event["uid"]]
    assert stored["visibility"] == "public"
    assert stored["payload"]["visibility"] == "public"
    assert stored["payload"]["privacy"] == "public"
    assert stored["payload"]["availability"] == "free"
    assert stored["payload"]["hide_from_freebusy"] is True
    assert stored["payload"]["description"] == "Updated description"


def test_event_description_must_be_text():
    fsm = CalendarFSM()
    event = request()
    event["payload"]["description"] = {"text": "not a string"}

    result = fsm.apply(event)

    assert result["code"] == 422
    assert result["result"]["reason"] == "invalid_event_description"


def test_sync_limits_are_capped_and_snapshot_can_be_omitted_from_pages():
    fsm = CalendarFSM()
    assert fsm.apply(request())["code"] == 200

    page = fsm.sync_for("alice", limit=1, include_snapshot=False)

    assert "snapshot" not in page
    with pytest.raises(ValueError, match="invalid_sync_limit"):
        fsm.sync(0, 1001)


def test_msg_id_deduplication_is_scoped_to_sender():
    fsm = CalendarFSM()
    shared_msg_id = "01J00000000000000000000081"
    alice = request(msg_id=shared_msg_id)
    assert fsm.apply(alice)["code"] == 200

    bob = request(
        msg_id=shared_msg_id,
        sender="bob",
        uid="bob:01J00000000000000000000082",
    )
    assert fsm.apply(bob)["code"] == 200

    repeated_bob = request(
        msg_id=shared_msg_id,
        sender="bob",
        uid="bob:01J00000000000000000000083",
    )
    assert fsm.apply(repeated_bob)["deduplicated"] is True
    assert len(fsm.events) == 2
    assert fsm.applied_index == 2


def test_reanchor_applies_a_counter_and_clears_stale_counters():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob"],
    }
    assert fsm.apply(proposal)["code"] == 200
    counter = request(
        msg_id="01J00000000000000000000084",
        sender="bob",
        uid=proposal["uid"],
        sequence=2,
    )
    counter["type"] = "COUNTER"
    counter["payload"] = {
        "proposed_start": "2026-11-21T12:00:00Z",
        "proposed_end": "2026-11-21T13:00:00Z",
    }
    assert fsm.apply(counter)["code"] == 200

    reanchor = request(
        msg_id="01J00000000000000000000085",
        uid=proposal["uid"],
        sequence=3,
    )
    reanchor["type"] = "REANCHOR"
    reanchor["payload"] = {"counter_user": "bob"}
    result = fsm.apply(reanchor)

    event = fsm.events[proposal["uid"]]
    assert result["code"] == 200
    assert event["sequence"] == 3
    assert event["state"] == "PROPOSED"
    assert event["payload"]["start"] == counter["payload"]["proposed_start"]
    assert fsm.counters.get(proposal["uid"]) is None
    assert fsm.attendance[proposal["uid"]] == {}


def test_decline_counter_removes_only_the_selected_proposal():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob", "carol"],
    }
    fsm.apply(proposal)
    for sequence, sender, msg_id in (
        (2, "bob", "01J00000000000000000000086"),
        (3, "carol", "01J00000000000000000000087"),
    ):
        counter = request(msg_id=msg_id, sender=sender, uid=proposal["uid"], sequence=sequence)
        counter["type"] = "COUNTER"
        counter["payload"] = {
            "proposed_start": "2026-11-21T12:00:00Z",
            "proposed_end": "2026-11-21T13:00:00Z",
        }
        assert fsm.apply(counter)["code"] == 200

    decline = request(
        msg_id="01J00000000000000000000088",
        uid=proposal["uid"],
        sequence=4,
    )
    decline["type"] = "DECLINECOUNTER"
    decline["payload"] = {"counter_user": "bob"}

    assert fsm.apply(decline)["code"] == 200
    assert set(fsm.counters[proposal["uid"]]) == {"carol"}
    assert fsm.events[proposal["uid"]]["sequence"] == 4


def test_editing_event_clears_old_counters():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "participants": ["bob"],
    }
    fsm.apply(proposal)
    fsm.counters[proposal["uid"]] = {"bob": {"proposed_start": "old"}}
    update = request(
        msg_id="01J00000000000000000000089",
        uid=proposal["uid"],
        sequence=2,
    )
    update["payload"] = {
        **proposal["payload"],
        "start": "2026-11-21T14:00:00Z",
        "end": "2026-11-21T15:00:00Z",
    }

    assert fsm.apply(update)["code"] == 200
    assert proposal["uid"] not in fsm.counters


def test_evaluate_expires_proposals_with_valid_system_time():
    fsm = CalendarFSM()
    proposal = request()
    proposal["payload"] = {
        "kind": "group",
        "start": "2026-11-21T10:00:00Z",
        "end": "2026-11-21T11:00:00Z",
        "deadline_utc": "2026-11-20T10:00:00Z",
        "participants": ["bob"],
    }
    assert fsm.apply(proposal)["code"] == 200
    fsm.counters[proposal["uid"]] = {"bob": {"proposed_start": "stale"}}
    evaluation = request(
        msg_id="01J00000000000000000000090",
        sender="system",
        uid="system:calendar",
        sequence=0,
    )
    evaluation["type"] = "EVALUATE"
    evaluation["key_id"] = ""
    evaluation["payload"] = {"now": "2026-11-21T00:00:00Z"}

    result = fsm.apply(evaluation)

    assert result["code"] == 200
    assert result["result"]["expired"] == [proposal["uid"]]
    assert fsm.events[proposal["uid"]]["state"] == "EXPIRED"
    assert fsm.events[proposal["uid"]]["sequence"] == 2
    assert proposal["uid"] not in fsm.counters


@pytest.mark.parametrize(
    "sender,payload,expected_reason",
    [
        ("alice", {"now": "2026-11-21T00:00:00Z"}, "system_command_only"),
        ("system", {"now": "not-a-date"}, "invalid_evaluation_time"),
    ],
)
def test_evaluate_rejects_non_system_or_invalid_time(sender, payload, expected_reason):
    fsm = CalendarFSM()
    evaluation = request(
        msg_id="01J00000000000000000000091",
        sender=sender,
        uid="system:calendar",
        sequence=0,
    )
    evaluation["type"] = "EVALUATE"
    evaluation["key_id"] = ""
    evaluation["payload"] = payload

    result = fsm.apply(evaluation)

    assert result["code"] in (403, 422)
    assert result["result"]["reason"] == expected_reason
    assert fsm.applied_index == 0


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
