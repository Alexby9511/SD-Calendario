import json

import pytest

from pkg.protocol.envelope import MAX_SEQUENCE, Envelope


def envelope(**overrides):
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


@pytest.mark.parametrize(
    "value",
    [float("inf"), float("nan"), 1.5, True, False, "7", None, [1], {"n": 1}],
)
def test_sequence_must_be_a_plain_integer(value):
    with pytest.raises(ValueError):
        Envelope.from_dict(envelope(sequence=value))


@pytest.mark.parametrize("value", [float("inf"), 2.5, True, "3"])
def test_seen_sequence_must_be_a_plain_integer(value):
    with pytest.raises(ValueError):
        Envelope.from_dict(envelope(seen_sequence=value))


@pytest.mark.parametrize("value", [-1, MAX_SEQUENCE + 1])
def test_sequences_out_of_range_are_rejected(value):
    with pytest.raises(ValueError):
        Envelope.from_dict(envelope(sequence=value))


def test_json_overflow_numbers_are_rejected_instead_of_crashing():
    # json.loads accepts "1e999" and returns inf; int(inf) used to raise
    # OverflowError, which escaped the state-machine adapter and wedged the
    # replicated log on every replica.
    parsed = json.loads('{"sequence": 1e999}')
    assert parsed["sequence"] == float("inf")

    with pytest.raises(ValueError):
        Envelope.from_dict(envelope(sequence=parsed["sequence"]))


def test_integer_sequences_are_preserved():
    parsed = Envelope.from_dict(envelope(sequence=7, seen_sequence=3))

    assert parsed.sequence == 7
    assert parsed.seen_sequence == 3


def test_missing_sequence_is_still_a_protocol_error():
    data = envelope()
    del data["sequence"]

    with pytest.raises(KeyError):
        Envelope.from_dict(data)


@pytest.mark.parametrize(
    "field",
    ["deployment_id", "group_id", "msg_id", "uid", "key_id", "client_sent_at"],
)
def test_lone_surrogates_in_envelope_fields_are_rejected(field):
    # json.loads accepts "\ud800", but neither SQLite nor canonical_bytes can
    # encode it. It must fail validation, not halfway through the log write.
    parsed = json.loads(json.dumps({"value": "\ud800"}))["value"]

    with pytest.raises(ValueError):
        Envelope.from_dict(envelope(**{field: parsed})).validate()


def test_lone_surrogate_in_payload_is_rejected():
    parsed = json.loads(json.dumps({"value": "\ud800"}))["value"]

    with pytest.raises(ValueError):
        Envelope.from_dict(envelope(payload={"title": parsed})).validate()


def test_non_ascii_payload_still_validates():
    Envelope.from_dict(envelope(payload={"title": "Diseño"})).validate()
