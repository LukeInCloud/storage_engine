"""Raft consensus (Ongaro & Ousterhout) with a durable log, pre-vote,
ReadIndex linearizable reads and log compaction with fuzzy snapshots, on top
of a pluggable state machine and transport.

Membership is dynamic: the initial set of node IDs comes from Config, later
changes (single-server additions via learners) are replicated as log entries
of kind KIND_CONFIG.
"""

from dataclasses import dataclass, field
from typing import List, NamedTuple, Optional


KIND_NORMAL = 0  # a state machine command (data) or a no-op (data is None)
KIND_CONFIG = 1  # an encoded raft.Membership; handled by Raft, never given to the state machine


@dataclass
class Entry:
    term: int
    index: int
    data: Optional[bytes] = None  # None = no-op (appended by a new leader)
    kind: int = KIND_NORMAL


class KV(NamedTuple):
    key: bytes
    value: bytes


@dataclass
class VoteReq:
    term: int
    candidate: str
    last_log_index: int
    last_log_term: int
    pre_vote: bool = False


@dataclass
class VoteResp:
    term: int
    granted: bool = False


@dataclass
class AppendReq:
    term: int
    leader: str
    prev_index: int
    prev_term: int
    entries: List[Entry] = field(default_factory=list)
    commit: int = 0


@dataclass
class AppendResp:
    term: int
    success: bool = False
    conflict_index: int = 0  # hint for the leader on failure
    conflict_term: int = 0


@dataclass
class SnapReq:
    """One chunk of a (fuzzy) snapshot of the state machine. Chunks of a
    session arrive in order (seq 0, 1, 2...); done marks the last one."""

    term: int
    leader: str
    session: int
    seq: int
    snap_index: int  # log index the snapshot is anchored at
    snap_term: int
    items: List[KV] = field(default_factory=list)
    done: bool = False
    config: bytes = b""  # encoded Membership in effect at snap_index


@dataclass
class PullReq:
    """A new node asks a peer for a copy of its state (see Node.bootstrap).
    cursor is None for the first chunk, then the previous response's next_cursor."""

    cursor: Optional[bytes] = None


@dataclass
class PullResp:
    items: List[KV] = field(default_factory=list)
    next_cursor: Optional[bytes] = None
    done: bool = False
    # Anchor of the whole snapshot; set in the first response only (snap_index 0 = "as before").
    snap_index: int = 0
    snap_term: int = 0
    config: bytes = b""


@dataclass
class SnapResp:
    term: int
    ok: bool = False
    skip: bool = False  # follower is already at or beyond snap_index; stop streaming


class Transport:
    """Delivers RPCs to peers. Implementations must enforce their own timeouts
    and may raise for any failure."""

    def request_vote(self, to: str, req: VoteReq) -> VoteResp:
        raise NotImplementedError

    def append_entries(self, to: str, req: AppendReq) -> AppendResp:
        raise NotImplementedError

    def install_snapshot(self, to: str, req: SnapReq) -> SnapResp:
        raise NotImplementedError


class StateMachine:
    """The replicated application.

    Commands must be deterministic and *idempotent when replayed in order*:
    snapshots are fuzzy (taken while the state keeps changing) and are repaired
    by replaying the log after the snapshot index, which converges only for
    such commands (e.g. puts and deletes).
    """

    def applied(self):
        """Return (index, term) of the last durably applied command."""
        raise NotImplementedError

    def apply(self, index: int, term: int, cmds: List[bytes]) -> None:
        """Atomically apply cmds (in order) and record (index, term) as
        applied. cmds may be empty."""
        raise NotImplementedError

    def sync(self) -> int:
        """Make all applied commands durable and return the highest index that
        is covered. apply() itself may be lazy about durability: the Raft log
        keeps every command that is not yet durably applied, so a crash replays
        them. The node calls sync() in the background."""
        raise NotImplementedError

    def snapshot_scan(self, cursor: Optional[bytes], max_bytes: int):
        """Stream the state: pass None first, then `next`, until done.
        Returns (items, next_cursor, done)."""
        raise NotImplementedError

    def restore_begin(self) -> None:
        """Start building a replacement state off to the side."""
        raise NotImplementedError

    def restore_chunk(self, items: List[KV]) -> None:
        raise NotImplementedError

    def restore_finish(self, index: int, term: int) -> None:
        """Atomically swap the new state in, marking it applied at (index, term)."""
        raise NotImplementedError

    def restore_abort(self) -> None:
        raise NotImplementedError


class RaftError(Exception):
    pass


class RaftTimeoutError(RaftError):
    def __init__(self):
        super().__init__("raft: timed out")


class StoppedError(RaftError):
    def __init__(self):
        super().__init__("raft: node stopped")


class TooBigError(RaftError):
    def __init__(self):
        super().__init__("raft: command too large")


class NotLeaderError(RaftError):
    """The node cannot serve the request. leader is the ID of the current
    leader if known."""

    def __init__(self, leader: str = ""):
        super().__init__("raft: not the leader (leader=%r)" % leader)
        self.leader = leader
