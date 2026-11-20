from server.ray_app.fsm import CalendarFSM


def test_same_msg_id_is_applied_once():
    fsm = CalendarFSM()
    command = {
        "deployment_id": "dep1",
        "group_id": "team",
        "msg_id": "01J00000000000000000000003",
        "type": "REQUEST",
        "uid": "alice:01J00000000000000000000004",
        "sender_id": "alice",
        "key_id": "key1",
        "sequence": 1,
        "seen_sequence": 0,
        "client_sent_at": "2026-11-20T00:00:00Z",
        "payload": {"title": "Una vez"},
    }
    first = fsm.apply(command)
    second = fsm.apply(command)
    assert first["deduplicated"] is False
    assert second["deduplicated"] is True
    assert fsm.applied_index == 1
