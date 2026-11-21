import pytest

from server.ray_app.consensus.log import RaftLog
from server.ray_app.consensus.messages import LogEntry


def make_entry(index: int, term: int = 1, command=None) -> LogEntry:
    return LogEntry(index=index, term=term, command=command or {"msg_id": f"m{index}"})


def test_empty_log_starts_at_snapshot():
    log = RaftLog(snapshot_index=4, snapshot_term=2)
    assert log.last_index == 4
    assert log.last_term == 2
    assert log.term_at(4) == 2
    assert log.entry(4) is None
    with pytest.raises(IndexError):
        log.term_at(5)


def test_append_requires_consecutive_indexes():
    log = RaftLog()
    log.append(make_entry(1))
    log.append(make_entry(2))
    assert [entry.index for entry in log.entries] == [1, 2]
    with pytest.raises(ValueError):
        log.append(make_entry(4))


def test_term_at_returns_entry_term():
    log = RaftLog()
    log.append(make_entry(1, term=1))
    log.append(make_entry(2, term=3))
    assert log.term_at(1) == 1
    assert log.term_at(2) == 3


def test_append_entries_appends_after_matching_previous():
    log = RaftLog()
    log.append(make_entry(1, term=1))
    assert log.append_entries(1, 1, [make_entry(2, term=2), make_entry(3, term=2)])
    assert log.last_index == 3
    assert log.last_term == 2


def test_append_entries_rejects_unknown_previous_index():
    log = RaftLog()
    assert not log.append_entries(3, 1, [make_entry(4)])
    assert log.last_index == 0


def test_append_entries_rejects_term_mismatch():
    log = RaftLog()
    log.append(make_entry(1, term=1))
    assert not log.append_entries(1, 2, [make_entry(2)])
    assert log.last_index == 1


def test_append_entries_truncates_conflicting_suffix():
    log = RaftLog()
    log.append(make_entry(1, term=1))
    log.append(make_entry(2, term=2))
    log.append(make_entry(3, term=2))
    assert log.append_entries(1, 1, [make_entry(2, term=3), make_entry(3, term=3)])
    assert [entry.term for entry in log.entries] == [1, 3, 3]
    assert log.last_index == 3


def test_append_entries_is_idempotent_for_same_term():
    log = RaftLog()
    log.append(make_entry(1, term=1))
    log.append(make_entry(2, term=2))
    assert log.append_entries(1, 1, [make_entry(2, term=2)])
    assert log.last_index == 2
    assert [entry.index for entry in log.entries] == [1, 2]


def test_append_entries_rejects_compacted_previous_index():
    log = RaftLog()
    log.append(make_entry(1))
    log.append(make_entry(2))
    log.compact(2)
    assert not log.append_entries(1, 1, [make_entry(3)])
    assert log.last_index == 2


def test_slice_returns_requested_range():
    log = RaftLog()
    for index in range(1, 5):
        log.append(make_entry(index))
    assert [entry.index for entry in log.slice(2, 4)] == [2, 3]
    assert [entry.index for entry in log.slice(3)] == [3, 4]


def test_truncate_cannot_remove_compacted_entries():
    log = RaftLog()
    log.append(make_entry(1))
    log.append(make_entry(2))
    log.compact(1)
    with pytest.raises(ValueError):
        log.truncate_from(1)
    log.truncate_from(2)
    assert log.last_index == 1


def test_compact_moves_snapshot_base():
    log = RaftLog()
    for index in range(1, 4):
        log.append(make_entry(index, term=index))
    log.compact(2)
    assert log.snapshot_index == 2
    assert log.snapshot_term == 2
    assert log.last_index == 3
    assert log.last_term == 3
    assert log.entry(2) is None
    assert log.term_at(2) == 2
