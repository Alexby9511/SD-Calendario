"""Single-node Raft state machine: pre-vote, elections and heartbeats.

The node is transport- and clock-agnostic so the consensus core can be tested
without Ray. It is intentionally single-threaded: the Ray wrapper must serialize
calls (or guard them with a lock) before adding a background timer thread.
"""

import random
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Optional, Set, Tuple

from ..logging_config import get_logger
from .clock import Clock, RealClock
from .log import RaftLog
from .messages import (
    AppendEntries,
    AppendEntriesResponse,
    InstallSnapshot,
    InstallSnapshotResponse,
    LogEntry,
    PreVote,
    PreVoteResponse,
    RequestVote,
    RequestVoteResponse,
)
from .storage import Storage
from .transport import TransportError

DEFAULT_SNAPSHOT_THRESHOLD = 1000
_INTERNAL_NOOP = {"_raft_internal": "noop"}


class Role(str, Enum):
    """Raft role of a replica."""

    FOLLOWER = "follower"
    PRE_CANDIDATE = "pre_candidate"
    CANDIDATE = "candidate"
    LEADER = "leader"


def _msg_id(command: Any) -> str:
    """Return the command ``msg_id`` for logs, or ``"-"`` when absent."""
    if isinstance(command, dict):
        value = command.get("msg_id")
        if value is not None:
            return str(value)
    return "-"


class NotLeaderError(Exception):
    """Raised when a client command is sent to a non-leader replica."""

    def __init__(self, leader_id: Optional[str] = None) -> None:
        self.leader_id = leader_id
        super().__init__("raft: not the leader")


class RaftNode:
    """Raft replica with elections, replication, commit, apply and snapshots.

    State is in memory unless ``storage`` is provided, in which case the term,
    vote, log and snapshot survive restarts.
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
        storage: Optional[Storage] = None,
        snapshot_provider: Optional[Callable[[], bytes]] = None,
        snapshot_restorer: Optional[Callable[[bytes], None]] = None,
        snapshot_threshold: int = DEFAULT_SNAPSHOT_THRESHOLD,
    ) -> None:
        if (
            not isinstance(snapshot_threshold, int)
            or isinstance(snapshot_threshold, bool)
            or snapshot_threshold < 1
        ):
            raise ValueError("raft: snapshot threshold must be a positive integer")
        self.node_id = node_id
        self.peers = [peer for peer in peers if peer != node_id]
        self.transport = transport
        self.clock = clock or RealClock()
        self.storage = storage
        self._snapshot_provider = snapshot_provider
        self._snapshot_restorer = snapshot_restorer
        self._snapshot_threshold = snapshot_threshold
        self._snapshot_data: Optional[bytes] = None
        self.log = RaftLog()
        self.role = Role.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        if storage is not None:
            meta = storage.load_meta()
            self.current_term = int(meta.get("current_term", 0))
            self.voted_for = meta.get("voted_for")
            self.log = RaftLog(
                storage.load_log(),
                snapshot_index=int(meta.get("snapshot_index", 0)),
                snapshot_term=int(meta.get("snapshot_term", 0)),
            )
        self.leader_id: Optional[str] = None
        self.commit_index = 0
        self.last_applied = 0
        self._apply_command = apply_command or (lambda command: None)
        self._results: Dict[int, Any] = {}
        self._election_timeout = election_timeout
        self._heartbeat_interval = heartbeat_interval
        self._minimum_election_timeout = min(election_timeout)
        self._random = random_source or random.Random()
        self._votes: Set[str] = set()
        self._pre_votes: Set[str] = set()
        self._pre_vote_term: Optional[int] = None
        self._next_index: Dict[str, int] = {}
        self._match_index: Dict[str, int] = {}
        self._election_deadline = 0.0
        self._heartbeat_deadline = 0.0
        self._last_leader_contact: Optional[float] = None
        self.logger = get_logger("raft.node")
        self._reset_election_deadline()
        if storage is not None:
            self._restore_stored_snapshot()

    def _restore_stored_snapshot(self) -> None:
        snapshot = self.storage.load_snapshot() if self.storage is not None else None
        if snapshot is None:
            return
        index, term, data = snapshot
        if index != self.log.snapshot_index or term != self.log.snapshot_term:
            raise ValueError("raft: snapshot and meta disagree")
        self._snapshot_data = data
        if self._snapshot_restorer is None:
            raise ValueError(
                "raft: snapshot found but no snapshot restorer is configured"
            )
        self._snapshot_restorer(data)
        self.commit_index = index
        self.last_applied = index

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
        """Run a pre-vote round and start a real election after a quorum.

        The current term and vote are left untouched during the pre-vote. A
        partitioned or restarted replica therefore cannot force a healthy
        leader to step down merely by reconnecting.
        """
        if self.role is Role.LEADER:
            return
        if self._leader_lease_active():
            self._reset_election_deadline()
            return

        prospective_term = self.current_term + 1
        self.role = Role.PRE_CANDIDATE
        self.leader_id = None
        self._pre_vote_term = prospective_term
        self._pre_votes = {self.node_id}
        self._reset_election_deadline()

        message = PreVote(
            prospective_term=prospective_term,
            candidate_id=self.node_id,
            last_log_index=self.log.last_index,
            last_log_term=self.log.last_term,
        )
        for peer in self.peers:
            try:
                response = self.transport.send(self.node_id, peer, message)
            except TransportError:
                continue
            self.handle_pre_vote_response(peer, response)
            if self.role is not Role.PRE_CANDIDATE:
                break
        if self.role is Role.PRE_CANDIDATE and len(self._pre_votes) >= self.quorum:
            self._start_candidate_term()

    def _start_candidate_term(self) -> None:
        """Increment the term and request binding votes after a pre-vote."""
        if self.role is not Role.PRE_CANDIDATE:
            return
        if self._pre_vote_term != self.current_term + 1:
            return
        if len(self._pre_votes) < self.quorum:
            return

        self.current_term = self._pre_vote_term
        self.role = Role.CANDIDATE
        self.voted_for = self.node_id
        self.leader_id = None
        self._votes = {self.node_id}
        self._pre_votes = set()
        self._pre_vote_term = None
        self._persist_meta()
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
        if self.role is Role.CANDIDATE and len(self._votes) >= self.quorum:
            self._become_leader()

    def propose(self, command: Dict[str, Any]) -> Any:
        """Append a client command and return its result once committed.

        Returns ``None`` when the command could not be committed in this round
        (for example, without quorum). The entry stays in the log and a later
        heartbeat can commit it.
        """
        if command == _INTERNAL_NOOP:
            raise ValueError("raft: internal no-op cannot be proposed by a client")
        return self._append_and_replicate(command)

    def _append_and_replicate(self, command: Dict[str, Any]) -> Any:
        if self.role is not Role.LEADER:
            raise NotLeaderError(self.leader_id)
        entry = LogEntry(
            index=self.log.last_index + 1,
            term=self.current_term,
            command=command,
        )
        # Persist before mutating the in-memory log: if the write fails, the
        # entry never exists in memory and the replica does not drift from disk.
        self._persist_log([entry])
        self.log.append(entry)
        self._replicate_all()
        self._advance_commit()
        if self.commit_index >= entry.index:
            self.logger.debug(
                "committed command node_id=%s index=%d term=%d msg_id=%s",
                self.node_id,
                entry.index,
                entry.term,
                _msg_id(entry.command),
            )
            return self._results.get(entry.index)
        self.logger.warning(
            "command pending without quorum node_id=%s index=%d term=%d msg_id=%s",
            self.node_id,
            entry.index,
            entry.term,
            _msg_id(entry.command),
        )
        return None

    def take_snapshot(self) -> bool:
        """Compact the applied prefix and persist a state-machine snapshot.

        Returns ``False`` when there is nothing new to compact. The snapshot
        provider must return ``bytes``; the format belongs to the state machine.
        """
        if self._snapshot_provider is None:
            raise RuntimeError("raft: snapshot provider is not configured")
        index = self.last_applied
        if index <= self.log.snapshot_index:
            return False
        term = self.log.term_at(index)
        data = self._snapshot_provider()
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("raft: snapshot provider must return bytes")
        data = bytes(data)
        self._persist_snapshot(index, term, data)
        self.log.compact(index)
        self._snapshot_data = data
        self._results = {
            key: value for key, value in self._results.items() if key >= index
        }
        self.logger.info(
            "compacted snapshot node_id=%s index=%d term=%d",
            self.node_id,
            index,
            term,
        )
        return True

    def handle_message(self, message: Any) -> Any:
        """Dispatch an incoming RPC to its handler."""
        if isinstance(message, PreVote):
            return self.handle_pre_vote(message)
        if isinstance(message, RequestVote):
            return self.handle_request_vote(message)
        if isinstance(message, AppendEntries):
            return self.handle_append_entries(message)
        if isinstance(message, InstallSnapshot):
            return self.handle_install_snapshot(message)
        raise TypeError(f"raft: unsupported message {type(message).__name__}")

    def handle_pre_vote(self, message: PreVote) -> PreVoteResponse:
        """Answer whether this replica would vote in the prospective term.

        Handling a pre-vote never changes the current term or the recorded
        vote. A voter that has heard from the current leader during the
        baseline election timeout also refuses the pre-vote, so a recovering
        replica cannot disrupt a healthy leader.
        """
        if message.prospective_term <= self.current_term:
            return PreVoteResponse(self.current_term, False)
        if self.role is Role.LEADER or self._leader_lease_active():
            return PreVoteResponse(self.current_term, False)

        # The vote is for a new prospective term, so a vote recorded in the
        # current term does not constrain it. Log freshness still applies.
        up_to_date = (
            message.last_log_term > self.log.last_term
            or (
                message.last_log_term == self.log.last_term
                and message.last_log_index >= self.log.last_index
            )
        )
        return PreVoteResponse(self.current_term, up_to_date)

    def handle_pre_vote_response(
        self, peer: str, response: PreVoteResponse
    ) -> None:
        """Count a pre-vote response without changing the current term."""
        if response.term > self.current_term:
            self._become_follower(response.term)
            return
        if self.role is not Role.PRE_CANDIDATE:
            return
        if response.term != self.current_term:
            return
        if response.vote_granted:
            self._pre_votes.add(peer)

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
            self._persist_meta()
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
        self._last_leader_contact = self.clock.now()
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

        replace_from = next(
            (
                entry.index
                for entry in message.entries
                if (existing := self.log.entry(entry.index)) is not None
                and existing.term != entry.term
            ),
            None,
        )
        # Build the post-append log off to the side and persist it before
        # swapping it in: if the write fails, memory must not run ahead of disk.
        candidate = RaftLog(
            self.log.entries, self.log.snapshot_index, self.log.snapshot_term
        )
        if not candidate.append_entries(
            message.prev_log_index, message.prev_log_term, message.entries
        ):
            return AppendEntriesResponse(self.current_term, False)
        if message.entries:
            if replace_from is None:
                self._persist_log(message.entries)
            else:
                self._persist_log(
                    candidate.slice(replace_from), truncate_from=replace_from
                )
            self.log = candidate

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
        if message.term < self.current_term:
            return InstallSnapshotResponse(self.current_term, False)
        if message.term > self.current_term:
            self._become_follower(message.term)
        else:
            self.role = Role.FOLLOWER
        self.leader_id = message.leader_id
        self._last_leader_contact = self.clock.now()
        self._reset_election_deadline()

        if message.last_included_index <= self.log.snapshot_index:
            return InstallSnapshotResponse(self.current_term, True)
        if message.last_included_index <= self.commit_index:
            return InstallSnapshotResponse(self.current_term, True)

        self._restore_snapshot(message.data)
        self._persist_snapshot(
            message.last_included_index,
            message.last_included_term,
            message.data,
        )
        self.log = RaftLog(
            snapshot_index=message.last_included_index,
            snapshot_term=message.last_included_term,
        )
        self.commit_index = message.last_included_index
        self.last_applied = message.last_included_index
        self._snapshot_data = message.data
        self._results = {}
        self._persist_log(replace=True)
        self.logger.info(
            "installed snapshot node_id=%s index=%d term=%d",
            self.node_id,
            message.last_included_index,
            message.last_included_term,
        )
        return InstallSnapshotResponse(self.current_term, True)

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
                if not self._send_snapshot(peer):
                    return False
                continue
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

    def _send_snapshot(self, peer: str) -> bool:
        """Send the latest compacted state to a peer that fell behind."""
        if self._snapshot_data is None:
            return False
        message = InstallSnapshot(
            term=self.current_term,
            leader_id=self.node_id,
            last_included_index=self.log.snapshot_index,
            last_included_term=self.log.snapshot_term,
            data=self._snapshot_data,
        )
        try:
            response = self.transport.send(self.node_id, peer, message)
        except TransportError:
            return False
        if response.term > self.current_term:
            self._become_follower(response.term)
            return False
        if self.role is not Role.LEADER or response.term != self.current_term:
            return False
        if not response.success:
            return False
        self._next_index[peer] = self.log.snapshot_index + 1
        self._match_index[peer] = max(
            self._match_index.get(peer, 0), self.log.snapshot_index
        )
        self._advance_commit()
        return True

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
            result = (
                {"ok": True, "internal": "noop"}
                if entry.command == _INTERNAL_NOOP
                else self._apply_command(entry.command)
            )
            self.last_applied = index
            self._results[index] = result
        if (
            self._snapshot_provider is not None
            and self.last_applied - self.log.snapshot_index > self._snapshot_threshold
        ):
            self.take_snapshot()

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
            "snapshot_index": self.log.snapshot_index,
            "snapshot_term": self.log.snapshot_term,
        }

    def _become_follower(self, term: int) -> None:
        previous_role = self.role
        previous_term = self.current_term
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._last_leader_contact = None
            self._persist_meta()
        self.role = Role.FOLLOWER
        self.leader_id = None
        self._pre_votes = set()
        self._pre_vote_term = None
        self._reset_election_deadline()
        if previous_role is Role.LEADER:
            self.logger.info(
                "stepped down node_id=%s from_role=leader term=%d previous_term=%d",
                self.node_id,
                self.current_term,
                previous_term,
            )
        elif previous_role in (Role.CANDIDATE, Role.PRE_CANDIDATE):
            self.logger.info(
                "abandoned election node_id=%s from_role=%s term=%d previous_term=%d",
                self.node_id,
                previous_role.value,
                self.current_term,
                previous_term,
            )
        elif term > previous_term:
            self.logger.debug(
                "advanced term node_id=%s term=%d previous_term=%d",
                self.node_id,
                self.current_term,
                previous_term,
            )

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.node_id
        self._last_leader_contact = self.clock.now()
        self._pre_votes = set()
        self._pre_vote_term = None
        self._next_index = {peer: self.log.last_index + 1 for peer in self.peers}
        self._match_index = {peer: 0 for peer in self.peers}
        self._reset_heartbeat_deadline()
        self.logger.info(
            "became leader node_id=%s term=%d last_log_index=%d",
            self.node_id,
            self.current_term,
            self.log.last_index,
        )
        if self.log.last_index > self.last_applied:
            self._append_and_replicate(dict(_INTERNAL_NOOP))
        self.send_heartbeats()

    def _persist_meta(self) -> None:
        if self.storage is not None:
            self.storage.save_meta(
                self.current_term,
                self.voted_for,
                self.log.snapshot_index,
                self.log.snapshot_term,
            )

    def _persist_log(
        self,
        entries: Optional[Iterable[LogEntry]] = None,
        replace: bool = False,
        truncate_from: Optional[int] = None,
    ) -> None:
        if self.storage is not None:
            self.storage.save_log(
                self.log.entries if entries is None else entries,
                replace=replace,
                truncate_from=truncate_from,
            )

    def _persist_snapshot(self, index: int, term: int, data: bytes) -> None:
        if self.storage is not None:
            self.storage.save_snapshot(index, term, data)

    def _restore_snapshot(self, data: bytes) -> None:
        if self._snapshot_restorer is None:
            raise RuntimeError("raft: snapshot restorer is not configured")
        self._snapshot_restorer(data)

    def _reset_election_deadline(self) -> None:
        low, high = self._election_timeout
        self._election_deadline = self.clock.now() + self._random.uniform(low, high)

    def _leader_lease_active(self) -> bool:
        """Return whether a recent heartbeat still protects the current leader."""
        if self._last_leader_contact is None:
            return False
        elapsed = self.clock.now() - self._last_leader_contact
        return elapsed < self._minimum_election_timeout

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
