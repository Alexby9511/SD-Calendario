"""Single-node Raft state machine: elections and heartbeats.

The node is transport- and clock-agnostic so the consensus core can be tested
without Ray. It is intentionally single-threaded: the Ray wrapper must serialize
calls (or guard them with a lock) before adding a background timer thread.
"""

import random
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Optional, Set, Tuple

from .clock import Clock, RealClock
from .log import RaftLog
from .messages import (
    AppendEntries,
    AppendEntriesResponse,
    InstallSnapshot,
    InstallSnapshotResponse,
    LogEntry,
    RequestVote,
    RequestVoteResponse,
)
from .transport import TransportError


class Role(str, Enum):
    """Raft role of a replica."""

    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class NotLeaderError(Exception):
    """Raised when a client command is sent to a non-leader replica."""

    def __init__(self, leader_id: Optional[str] = None) -> None:
        self.leader_id = leader_id
        super().__init__("raft: not the leader")


class RaftNode:
    """Raft replica with elections and heartbeats.

    Log replication and commit/apply are not implemented yet; ``AppendEntries``
    is accepted and stored so the next step can build on it.
    """

    def __init__(
        self,
        node_id: str,
        peers: Iterable[str],
        transport: Any,
        clock: Optional[Clock] = None,
        election_timeout: Tuple[float, float] = (0.15, 0.3),
        heartbeat_interval: float = 0.05,
        random_source: Optional[random.Random] = None,
        apply_command: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ) -> None:
        self.node_id = node_id
        self.peers = [peer for peer in peers if peer != node_id]
        self.transport = transport
        self.clock = clock or RealClock()
        self.log = RaftLog()
        self.role = Role.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        self.commit_index = 0
        self.last_applied = 0
        self._apply_command = apply_command or (lambda command: None)
        self._results: Dict[int, Any] = {}
        self._election_timeout = election_timeout
        self._heartbeat_interval = heartbeat_interval
        self._random = random_source or random.Random()
        self._votes: Set[str] = set()
        self._next_index: Dict[str, int] = {}
        self._match_index: Dict[str, int] = {}
        self._election_deadline = 0.0
        self._heartbeat_deadline = 0.0
        self._reset_election_deadline()

    @property
    def quorum(self) -> int:
        """Number of votes needed to win an election."""
        return (len(self.peers) + 1) // 2 + 1

    def tick(self) -> None:
        """Advance timers; callers drive this from a thread or a test."""
        now = self.clock.now()
        if self.role is Role.LEADER:
            if now >= self._heartbeat_deadline:
                self._reset_heartbeat_deadline()
                self.send_heartbeats()
            return
        if now >= self._election_deadline:
            self.start_election()

    def start_election(self) -> None:
        """Become a candidate and request votes from every peer."""
        self.current_term += 1
        self.role = Role.CANDIDATE
        self.voted_for = self.node_id
        self.leader_id = None
        self._votes = {self.node_id}
        self._reset_election_deadline()

        message = RequestVote(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self.log.last_index,
            last_log_term=self.log.last_term,
        )
        for peer in self.peers:
            try:
                response = self.transport.send(self.node_id, peer, message)
            except TransportError:
                continue
            self.handle_request_vote_response(peer, response)
            if self.role is not Role.CANDIDATE:
                break

    def propose(self, command: Dict[str, Any]) -> Any:
        """Append a client command and return its result once committed.

        Returns ``None`` when the command could not be committed in this round
        (for example, without quorum). The entry stays in the log and a later
        heartbeat can commit it.
        """
        if self.role is not Role.LEADER:
            raise NotLeaderError(self.leader_id)
        entry = LogEntry(
            index=self.log.last_index + 1,
            term=self.current_term,
            command=command,
        )
        self.log.append(entry)
        self._replicate_all()
        self._advance_commit()
        if self.commit_index >= entry.index:
            return self._results.get(entry.index)
        return None

    def handle_message(self, message: Any) -> Any:
        """Dispatch an incoming RPC to its handler."""
        if isinstance(message, RequestVote):
            return self.handle_request_vote(message)
        if isinstance(message, AppendEntries):
            return self.handle_append_entries(message)
        if isinstance(message, InstallSnapshot):
            return self.handle_install_snapshot(message)
        raise TypeError(f"raft: unsupported message {type(message).__name__}")

    def handle_request_vote(self, message: RequestVote) -> RequestVoteResponse:
        if message.term < self.current_term:
            return RequestVoteResponse(self.current_term, False)
        if message.term > self.current_term:
            self._become_follower(message.term)

        up_to_date = (
            message.last_log_term > self.log.last_term
            or (
                message.last_log_term == self.log.last_term
                and message.last_log_index >= self.log.last_index
            )
        )
        can_vote = self.voted_for in (None, message.candidate_id)
        if can_vote and up_to_date:
            self.voted_for = message.candidate_id
            self._reset_election_deadline()
            return RequestVoteResponse(self.current_term, True)
        return RequestVoteResponse(self.current_term, False)

    def handle_request_vote_response(
        self, peer: str, response: RequestVoteResponse
    ) -> None:
        if response.term > self.current_term:
            self._become_follower(response.term)
            return
        if self.role is not Role.CANDIDATE or response.term != self.current_term:
            return
        if not response.vote_granted:
            return
        self._votes.add(peer)
        if len(self._votes) >= self.quorum:
            self._become_leader()

    def handle_append_entries(self, message: AppendEntries) -> AppendEntriesResponse:
        if message.term < self.current_term:
            return AppendEntriesResponse(self.current_term, False)
        if message.term > self.current_term:
            self._become_follower(message.term)
        else:
            self.role = Role.FOLLOWER
        self.leader_id = message.leader_id
        self._reset_election_deadline()

        if message.prev_log_index < self.log.snapshot_index:
            return AppendEntriesResponse(
                self.current_term,
                False,
                conflict_index=self.log.snapshot_index + 1,
            )
        if message.prev_log_index > self.log.last_index:
            return AppendEntriesResponse(
                self.current_term,
                False,
                conflict_index=self.log.last_index + 1,
            )
        if self.log.term_at(message.prev_log_index) != message.prev_log_term:
            return AppendEntriesResponse(
                self.current_term,
                False,
                conflict_index=self._first_index_of_term(message.prev_log_index),
            )

        if not self.log.append_entries(
            message.prev_log_index, message.prev_log_term, message.entries
        ):
            return AppendEntriesResponse(self.current_term, False)

        if message.leader_commit > self.commit_index:
            self.commit_index = min(message.leader_commit, self.log.last_index)
            self._apply_committed()
        return AppendEntriesResponse(
            self.current_term,
            True,
            match_index=message.prev_log_index + len(message.entries),
        )

    def handle_append_entries_response(
        self, peer: str, response: AppendEntriesResponse
    ) -> None:
        if response.term > self.current_term:
            self._become_follower(response.term)
            return
        if self.role is not Role.LEADER or response.term != self.current_term:
            return
        if response.success:
            self._match_index[peer] = response.match_index
            self._next_index[peer] = response.match_index + 1
            self._advance_commit()
            return
        current_next = self._next_index.get(peer, 1)
        conflict = response.conflict_index or (current_next - 1)
        self._next_index[peer] = max(1, min(current_next - 1, conflict))

    def handle_install_snapshot(
        self, message: InstallSnapshot
    ) -> InstallSnapshotResponse:
        raise NotImplementedError("raft: snapshot install is not implemented yet")

    def send_heartbeats(self) -> None:
        """Replicate pending entries (or send an empty heartbeat) to every peer."""
        self._replicate_all()

    def _replicate_all(self) -> None:
        for peer in self.peers:
            self._replicate(peer)
            if self.role is not Role.LEADER:
                break

    def _replicate(self, peer: str) -> bool:
        """Send pending entries to one peer, retrying on log mismatch."""
        while self.role is Role.LEADER:
            next_index = self._next_index.get(peer, self.log.last_index + 1)
            if next_index <= self.log.snapshot_index:
                return False  # snapshot install is not implemented yet
            prev_index = next_index - 1
            try:
                prev_term = self.log.term_at(prev_index)
            except IndexError:
                return False
            entries = (
                self.log.slice(next_index)
                if next_index <= self.log.last_index
                else []
            )
            message = AppendEntries(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_index,
                prev_log_term=prev_term,
                entries=entries,
                leader_commit=self.commit_index,
            )
            try:
                response = self.transport.send(self.node_id, peer, message)
            except TransportError:
                return False
            previous_next = next_index
            self.handle_append_entries_response(peer, response)
            if response.success:
                return True
            if self.role is not Role.LEADER:
                return False
            if self._next_index.get(peer, previous_next) >= previous_next:
                return False
        return False

    def _advance_commit(self) -> None:
        if self.role is not Role.LEADER:
            return
        for index in range(self.log.last_index, self.commit_index, -1):
            if self.log.term_at(index) != self.current_term:
                continue
            replicated = 1 + sum(
                1 for peer in self.peers if self._match_index.get(peer, 0) >= index
            )
            if replicated >= self.quorum:
                self.commit_index = index
                break
        self._apply_committed()

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            index = self.last_applied + 1
            entry = self.log.entry(index)
            if entry is None:
                raise RuntimeError(f"raft: missing entry at index {index}")
            result = self._apply_command(entry.command)
            self.last_applied = index
            self._results[index] = result

    def status(self) -> Dict[str, Any]:
        """Expose election state for debugging and the API status endpoint."""
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "term": self.current_term,
            "voted_for": self.voted_for,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "last_log_index": self.log.last_index,
            "last_log_term": self.log.last_term,
        }

    def _become_follower(self, term: int) -> None:
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.role = Role.FOLLOWER
        self.leader_id = None
        self._reset_election_deadline()

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.node_id
        self._next_index = {peer: self.log.last_index + 1 for peer in self.peers}
        self._match_index = {peer: 0 for peer in self.peers}
        self._reset_heartbeat_deadline()
        self.send_heartbeats()

    def _reset_election_deadline(self) -> None:
        low, high = self._election_timeout
        self._election_deadline = self.clock.now() + self._random.uniform(low, high)

    def _reset_heartbeat_deadline(self) -> None:
        self._heartbeat_deadline = self.clock.now() + self._heartbeat_interval

    def _first_index_of_term(self, index: int) -> int:
        term = self.log.term_at(index)
        first = index
        while (
            first > self.log.snapshot_index + 1
            and self.log.term_at(first - 1) == term
        ):
            first -= 1
        return first
