"""In-memory Raft log with an optional snapshot base."""

from typing import Iterable, List, Optional

from .messages import LogEntry


class RaftLog:
    """Ordered Raft log.

    Entries are 1-based and contiguous. A snapshot covers every index up to
    ``snapshot_index``; the log only stores entries after it.
    """

    def __init__(
        self,
        entries: Optional[Iterable[LogEntry]] = None,
        snapshot_index: int = 0,
        snapshot_term: int = 0,
    ) -> None:
        if snapshot_index < 0 or snapshot_term < 0:
            raise ValueError("raft: snapshot index and term must be non-negative")
        self.snapshot_index = snapshot_index
        self.snapshot_term = snapshot_term
        self._entries: List[LogEntry] = list(entries or [])
        self._validate_contiguous()

    def _validate_contiguous(self) -> None:
        expected = self.snapshot_index + 1
        for entry in self._entries:
            if entry.index != expected:
                raise ValueError("raft: log entries must be contiguous")
            expected += 1

    @property
    def entries(self) -> List[LogEntry]:
        """Return a copy of the stored entries."""
        return list(self._entries)

    @property
    def last_index(self) -> int:
        return self._entries[-1].index if self._entries else self.snapshot_index

    @property
    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else self.snapshot_term

    def term_at(self, index: int) -> int:
        """Return the term of ``index``; raise if it is outside the log."""
        if index == self.snapshot_index:
            return self.snapshot_term
        entry = self.entry(index)
        if entry is None:
            raise IndexError(f"raft: no entry at index {index}")
        return entry.term

    def entry(self, index: int) -> Optional[LogEntry]:
        """Return the entry at ``index`` or ``None`` if it is not stored."""
        offset = index - self.snapshot_index - 1
        if offset < 0 or offset >= len(self._entries):
            return None
        return self._entries[offset]

    def append(self, entry: LogEntry) -> None:
        """Append a single entry; indexes must be consecutive."""
        if entry.index != self.last_index + 1:
            raise ValueError("raft: entries must be appended in order")
        self._entries.append(entry)

    def append_entries(
        self,
        prev_log_index: int,
        prev_log_term: int,
        entries: Iterable[LogEntry],
    ) -> bool:
        """Replicate ``entries`` after ``prev_log_index``.

        Returns ``False`` when the previous entry does not match, so the leader
        can retry with an earlier index (or send a snapshot).
        """
        if prev_log_index < self.snapshot_index:
            return False
        if prev_log_index > self.last_index:
            return False
        if self.term_at(prev_log_index) != prev_log_term:
            return False

        for entry in entries:
            if entry.index <= prev_log_index:
                raise ValueError("raft: entry index must follow prev_log_index")
            existing = self.entry(entry.index)
            if existing is not None:
                if existing.term == entry.term:
                    continue
                self.truncate_from(entry.index)
            if entry.index != self.last_index + 1:
                raise ValueError("raft: gap in replicated entries")
            self._entries.append(entry)
        return True

    def truncate_from(self, index: int) -> None:
        """Drop every entry with ``index`` or greater."""
        if index <= self.snapshot_index:
            raise ValueError("raft: cannot truncate compacted entries")
        offset = index - self.snapshot_index - 1
        del self._entries[offset:]

    def slice(self, start: int, end: Optional[int] = None) -> List[LogEntry]:
        """Return entries with ``start <= index < end`` (``end`` exclusive)."""
        if start <= self.snapshot_index:
            raise ValueError("raft: start index is compacted")
        stop = self.last_index + 1 if end is None else end
        return [entry for entry in self._entries if start <= entry.index < stop]

    def compact(self, index: int) -> None:
        """Discard entries up to ``index`` after taking a snapshot there."""
        if index < self.snapshot_index:
            raise ValueError("raft: cannot compact backwards")
        if index > self.last_index:
            raise ValueError("raft: cannot compact beyond last index")
        self.snapshot_term = self.term_at(index)
        self.snapshot_index = index
        self._entries = [entry for entry in self._entries if entry.index > index]
