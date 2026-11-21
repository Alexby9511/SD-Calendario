"""Raft message types exchanged between coordinator replicas."""

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass(frozen=True)
class LogEntry:
    """A command stored in the replicated log."""

    index: int
    term: int
    command: Dict[str, Any]


@dataclass(frozen=True)
class PreVote:
    """Candidate asks whether peers would vote in a prospective term.

    ``prospective_term`` is the term the candidate would use if the pre-vote
    succeeds. A voter must not change its current term or vote when handling
    this message.
    """

    prospective_term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class PreVoteResponse:
    """Reply to :class:`PreVote`."""

    term: int
    vote_granted: bool


@dataclass(frozen=True)
class RequestVote:
    """Candidate asks a peer for its vote in ``term``."""

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteResponse:
    """Reply to :class:`RequestVote`."""

    term: int
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntries:
    """Leader replicates entries and acts as a heartbeat when empty."""

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass(frozen=True)
class AppendEntriesResponse:
    """Reply to :class:`AppendEntries`."""

    term: int
    success: bool
    match_index: int = 0
    conflict_index: int = 0


@dataclass(frozen=True)
class InstallSnapshot:
    """Leader sends a compacted state to a lagging follower."""

    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    data: bytes


@dataclass(frozen=True)
class InstallSnapshotResponse:
    """Reply to :class:`InstallSnapshot`."""

    term: int
    success: bool
