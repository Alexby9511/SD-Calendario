from server.ray_app.state import CalendarState


def command(**overrides):
    data = {
        "deployment_id": "dep1",
        "group_id": "team",
        "msg_id": "01J00000000000000000000000",
        "type": "REQUEST",
        "uid": "alice:01J00000000000000000000001",
        "sender_id": "alice",
        "key_id": "key1",
        "sequence": 1,
        "seen_sequence": 0,
        "client_sent_at": "2026-11-20T00:00:00Z",
        "payload": {"title": "Diseño"},
    }
    data.update(overrides)
    return data


def test_malformed_commands_are_rejected_without_raising():
    state = CalendarState()

    result = state.apply(command(sequence=float("inf")))

    assert result["code"] == 422
    assert result["applied_index"] == 0
    assert state.fsm.applied_index == 0


def test_the_state_machine_keeps_working_after_a_rejected_command():
    state = CalendarState()
    state.apply(command(sequence=float("inf")))

    result = state.apply(command(msg_id="01J00000000000000000000002"))

    assert result["code"] == 200
    assert state.fsm.applied_index == 1


def test_the_adapter_never_propagates_unexpected_exceptions(monkeypatch):
    state = CalendarState()

    def boom(_command):
        raise RuntimeError("boom")

    monkeypatch.setattr(state.fsm, "apply", boom)
    result = state.apply(command())

    assert result["code"] == 422
    assert result["result"]["reason"] == "RuntimeError"
