import logging
import os
import queue
import random
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from ..util import WaitGroup
from .membership import Membership, load_base, save_base
from .storage import LogStore, load_hard_state, save_hard_state
from .types import (KIND_CONFIG, KIND_NORMAL, AppendReq, AppendResp, Entry, KV, NotLeaderError,
                    PullReq, PullResp, RaftError, RaftTimeoutError, SnapReq, SnapResp,
                    StateMachine, StoppedError, TooBigError, Transport, VoteReq, VoteResp)

_log = logging.getLogger("raft")

FOLLOWER, CANDIDATE, LEADER = 0, 1, 2
ROLE_NAMES = ("follower", "candidate", "leader")

MAX_APPLY_BATCH = 1024


@dataclass
class Config:
    id: str = ""
    peers: List[str] = None  # IDs of ALL nodes in the cluster, including this one (initial bootstrap)
    dir: str = ""  # holds raft.log and state; created if missing

    # Dynamic membership. A node that has a persisted configuration (it was
    # added to a cluster earlier, or compacted its log) ignores peers.
    join: bool = False  # start as a non-member: no elections until a leader adds this node
    peer_info: Optional[Dict[str, str]] = None  # opaque per-node info (addresses) carried in configs
    # Called (with the node mutex held: do not block or call back into the
    # Node) whenever the configuration in effect changes, and once at start().
    on_config: Optional[Callable[[Membership], None]] = None

    transport: Transport = None
    sm: StateMachine = None

    # election_timeout is the minimum (seconds); the effective timeout is
    # random in [T, 2T).
    election_timeout: float = 0.0  # default 0.4
    heartbeat_interval: float = 0.0  # default election_timeout/8

    log_retain: int = 0  # applied entries kept in the log for slow followers (default 8192)
    max_batch_entries: int = 0  # entries per AppendEntries (default 512)
    max_batch_bytes: int = 0  # bytes per AppendEntries (default 4 MiB)
    max_command_bytes: int = 0  # largest accepted command (default 32 MiB)
    snapshot_chunk_bytes: int = 0  # default 1 MiB

    no_sync: bool = False  # skip fsync of the Raft log (tests only)
    logf: Optional[Callable] = None

    def set_defaults(self) -> None:
        if self.election_timeout <= 0:
            self.election_timeout = 0.4
        if self.heartbeat_interval <= 0:
            self.heartbeat_interval = self.election_timeout / 8
        if self.log_retain <= 0:
            self.log_retain = 8192
        if self.max_batch_entries <= 0:
            self.max_batch_entries = 512
        if self.max_batch_bytes <= 0:
            self.max_batch_bytes = 4 << 20
        if self.max_command_bytes <= 0:
            self.max_command_bytes = 32 << 20
        if self.snapshot_chunk_bytes <= 0:
            self.snapshot_chunk_bytes = 1 << 20
        if self.logf is None:
            self.logf = lambda *a, **k: None


class _Proposal:
    __slots__ = ("data", "term", "index", "err", "_ev")

    def __init__(self, data: bytes):
        self.data = data
        self.term = 0
        self.index = 0
        self.err = None
        self._ev = threading.Event()

    def complete(self, err) -> None:
        if not self._ev.is_set():
            self.err = err
            self._ev.set()

    def wait(self, timeout: float) -> bool:
        return self._ev.wait(timeout)


class _RestoreSession:
    __slots__ = ("id", "next_seq", "snap_idx", "snap_term", "started")

    def __init__(self, sid, snap_idx, snap_term):
        self.id = sid
        self.next_seq = 0
        self.snap_idx = snap_idx
        self.snap_term = snap_term
        self.started = False


@dataclass
class Status:
    id: str
    role: int
    term: int
    leader: str
    commit: int
    applied: int
    last_index: int
    log_start: int  # entries up to here were compacted away

    @property
    def role_name(self) -> str:
        return ROLE_NAMES[self.role]


class Node:
    def __init__(self, cfg: Config):
        cfg.set_defaults()
        if cfg.transport is None or cfg.sm is None or not cfg.id:
            raise ValueError("raft: id, transport and sm are required")
        self.cfg = cfg
        os.makedirs(cfg.dir, exist_ok=True)
        base = load_base(cfg.dir)
        if base is None:
            if cfg.join:
                # Persist "member of nothing" at once: a restart without the
                # join option must not turn this node into a cluster of one.
                base = (0, Membership())
                save_base(cfg.dir, 0, base[1])
            else:
                peers = cfg.peers or []
                if cfg.id not in peers:
                    raise ValueError("raft: peers must include this node's id")
                info = cfg.peer_info or {}
                base = (0, Membership({p: info.get(p, "") for p in peers}, peers))
        # Configuration: base is in effect at log index base_index; hist lists
        # the configuration entries in the log after it (the last one wins).
        self.base_index, self.base = base
        self.hist = []
        self.started = False

        self.mu = threading.Lock()
        self.cond = threading.Condition(self.mu)  # notified on any state change waiters care about

        self.stop_ev = threading.Event()
        self.wg = WaitGroup()
        self.stopped = False
        self.failed = None

        self.role = FOLLOWER
        self.term, self.voted_for = load_hard_state(cfg.dir)
        self.leader = ""

        self.last_contact = float("-inf")  # last valid message from a leader
        self.election_deadline = 0.0

        self.log = LogStore.open(cfg.dir, cfg.no_sync)
        try:
            ai, at = cfg.sm.applied()
            if ai > self.log.last_index():
                # The state machine is ahead of the log (a snapshot restore
                # finished but the log reset did not): the log is obsolete.
                self.log.reset(ai, at)
            elif ai < self.log.prev_index:
                raise RuntimeError("raft: state machine (applied %d) is behind the log start (%d)"
                                   % (ai, self.log.prev_index))
        except BaseException:
            self.log.close()
            raise
        self.hist = [(e.index, Membership.decode(e.data)) for e in self.log.entries
                     if e.kind == KIND_CONFIG and e.index > self.base_index]
        self.mem, self.config_index = (self.hist[-1][1], self.hist[-1][0]) if self.hist else (
            self.base, self.base_index)
        self.voters = self.mem.voters
        self.others = sorted(p for p in self.mem.members if p != cfg.id)
        self.commit = self.applied = self.sm_applied = self.durable = ai
        # applied: applied in memory (includes trailing no-ops)
        # sm_applied: last index handed to the state machine
        # durable: last index the state machine has made durable (log may be compacted up to here)
        self.applying = False
        self.restoring = None

        # leader state
        self.next = {}
        self.match = {}
        self.ack_time = {}  # send time of the newest request each peer acknowledged
        self.snap_pin = {}  # log must be kept from this index while a snapshot streams
        self.waiters = {}
        self.kick = {p: threading.Event() for p in self.others}

        self.prop_mu = threading.Lock()
        self.prop_q = []
        self.prop_sig = threading.Event()

        self.rnd = random.Random(int.from_bytes(os.urandom(8), "little") ^ hash(cfg.id))
        self._reset_election_timer()

    # ---- lifecycle ----

    def start(self) -> None:
        with self.mu:
            self.started = True
            if self.cfg.on_config is not None:
                self.cfg.on_config(self.mem)
        self.wg.go(self._election_loop, name="raft-election")
        self.wg.go(self._promote_loop, name="raft-promote")
        self.wg.go(self._apply_loop, name="raft-apply")
        self.wg.go(self._append_loop, name="raft-append")
        self.wg.go(self._sync_loop, name="raft-sync")
        for p in self.others:
            self.wg.go(self._replicator, p, name="raft-repl-" + p)

    def stop(self) -> None:
        with self.mu:
            if self.stopped:
                return
            self.stopped = True
            self.stop_ev.set()
            self.cond.notify_all()
        self.prop_sig.set()
        for ev in list(self.kick.values()):
            ev.set()
        self.wg.wait()
        with self.mu:
            self._fail_waiters(StoppedError())
        self._fail_queued(StoppedError())
        self.log.close()

    def status(self) -> Status:
        with self.mu:
            return Status(self.cfg.id, self.role, self.term, self.leader, self.commit, self.applied,
                          self.log.last_index(), self.log.prev_index)

    # ---- helpers (mu held unless noted) ----

    def _quorum(self) -> int:
        return len(self.voters) // 2 + 1

    def membership(self) -> Membership:
        """The configuration in effect (the latest in the log, committed or not)."""
        with self.mu:
            return self.mem

    def _vote_peers(self):
        return [p for p in self.voters if p != self.cfg.id], self._quorum()

    def _set_config(self, mem: Membership, index: int) -> None:
        """Make mem the configuration in effect. mu held."""
        self.mem, self.config_index, self.voters = mem, index, mem.voters
        self.others = sorted(p for p in mem.members if p != self.cfg.id)
        for p in [p for p in self.kick if p not in mem.members]:
            # Removed: its replicator thread notices and exits.
            self.kick.pop(p).set()
            for d in (self.next, self.match, self.ack_time, self.snap_pin):
                d.pop(p, None)
        for p in self.others:
            if p not in self.kick:
                self.kick[p] = threading.Event()
                if self.started and not self.stopped:
                    self.wg.go(self._replicator, p, name="raft-repl-" + p)
            if self.role == LEADER and p not in self.next:
                self.next[p] = self.log.last_index() + 1
                self.match[p] = 0
        self.cfg.logf("[%s] configuration at %d: %r", self.cfg.id, index, mem)
        if self.cfg.on_config is not None:
            self.cfg.on_config(mem)
        self.cond.notify_all()

    def _config_at(self, index: int) -> Membership:
        cur = self.base
        for i, m in self.hist:
            if i > index:
                break
            cur = m
        return cur

    def _config_entry_appended(self, e: Entry) -> None:
        try:
            m = Membership.decode(e.data or b"")
        except Exception as ex:
            self._fatal(ex)
            raise
        self.hist.append((e.index, m))
        self._set_config(m, e.index)

    def _log_truncated(self, index: int) -> None:
        """Entries from index on are gone: forget configurations they carried."""
        if self.hist and self.hist[-1][0] >= index:
            self.hist = [h for h in self.hist if h[0] < index]
            i, m = self.hist[-1] if self.hist else (self.base_index, self.base)
            self._set_config(m, i)

    def _fold_base(self, upto: int) -> bool:
        """Persist the configuration in effect at `upto` as the new base so
        the log entries that carried it may be dropped."""
        n = 0
        for k, (i, _) in enumerate(self.hist):
            if i <= upto:
                n = k + 1
        if n == 0:
            return True
        i, m = self.hist[n - 1]
        try:
            save_base(self.cfg.dir, i, m)
        except Exception as e:
            self._fatal(e)
            return False
        self.base_index, self.base = i, m
        self.hist = self.hist[n:]
        return True

    def _reset_election_timer(self) -> None:
        t = self.cfg.election_timeout
        self.election_deadline = time.monotonic() + t + self.rnd.random() * t

    def _fatal(self, err: BaseException) -> None:
        if self.failed is None:
            self.failed = err
            self.cfg.logf("[%s] FATAL: %s", self.cfg.id, err)
            _log.error("raft[%s]: fatal: %s", self.cfg.id, err)
        self.role = FOLLOWER
        self._fail_waiters(err)
        self.cond.notify_all()

    def _persist_hard_state(self) -> None:
        try:
            save_hard_state(self.cfg.dir, self.term, self.voted_for)
        except Exception as e:
            self._fatal(e)

    def _fail_waiters(self, err) -> None:
        waiters, self.waiters = self.waiters, {}
        for p in waiters.values():
            p.complete(err)

    def _not_leader(self) -> NotLeaderError:
        return NotLeaderError(self.leader)

    def _step_down(self, term: int) -> None:
        """Move to follower in `term` (>= current term)."""
        if term > self.term:
            self.term, self.voted_for, self.leader = term, "", ""
            self._persist_hard_state()
        if self.role == LEADER:
            self.cfg.logf("[%s] stepping down in term %d", self.cfg.id, self.term)
            self._fail_waiters(self._not_leader())
        self.role = FOLLOWER
        self._reset_election_timer()
        self.cond.notify_all()

    def _wait_until(self, deadline: float, pred) -> None:
        """Block (releasing mu) until pred() or the deadline/stop. mu held."""
        while not pred():
            if self.stopped:
                raise StoppedError()
            if self.failed is not None:
                raise self.failed
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RaftTimeoutError()
            self.cond.wait(remaining)

    def _term_of(self, index: int) -> int:
        t = self.log.term(index)
        return 0 if t is None else t

    # ---- elections ----

    def _election_loop(self) -> None:
        tick = self.cfg.election_timeout / 10
        while not self.stop_ev.wait(tick):
            with self.mu:
                # Learners (and nodes not in any configuration yet) never campaign.
                if (self.failed is None and self.role != LEADER and self.cfg.id in self.voters
                        and time.monotonic() > self.election_deadline):
                    self._reset_election_timer()
                    term = self.term + 1
                    li, lt = self.log.last_index(), self.log.last_term()
                    peers, quorum = self._vote_peers()
                    self.wg.go(self._run_election, term, li, lt, peers, quorum, name="raft-vote")

    def _gather(self, pre: bool, term: int, last_idx: int, last_term: int, peers, quorum) -> bool:
        """Ask the voting peers for votes and report whether a majority granted."""
        results = queue.Queue()

        def ask(p):
            try:
                resp = self.cfg.transport.request_vote(
                    p, VoteReq(term, self.cfg.id, last_idx, last_term, pre))
                results.put(resp)
            except Exception:
                results.put(None)

        for p in peers:
            threading.Thread(target=ask, args=(p,), daemon=True, name="raft-ask").start()
        votes = 1
        for _ in peers:
            while True:
                try:
                    resp = results.get(timeout=0.05)
                    break
                except queue.Empty:
                    if self.stop_ev.is_set():
                        return False
            if resp is None:
                continue
            if resp.term > term:
                with self.mu:
                    if resp.term > self.term:
                        self._step_down(resp.term)
                return False
            if resp.granted:
                votes += 1
            if votes >= quorum:
                return True
        return votes >= quorum

    def _run_election(self, term: int, last_idx: int, last_term: int, peers, quorum) -> None:
        if peers and not self._gather(True, term, last_idx, last_term, peers, quorum):
            return
        with self.mu:
            if self.failed is not None or self.role == LEADER or self.term + 1 != term:
                return
            self.term, self.voted_for, self.role, self.leader = term, self.cfg.id, CANDIDATE, ""
            self._persist_hard_state()
            self._reset_election_timer()
            last_idx, last_term = self.log.last_index(), self.log.last_term()
            peers, quorum = self._vote_peers()
        won = not peers or self._gather(False, term, last_idx, last_term, peers, quorum)
        with self.mu:
            if won and self.term == term and self.role == CANDIDATE and self.failed is None:
                self._become_leader()

    def _become_leader(self) -> None:
        self.cfg.logf("[%s] became leader in term %d", self.cfg.id, self.term)
        self.role, self.leader = LEADER, self.cfg.id
        self.next, self.match = {}, {}
        self.ack_time, self.snap_pin = {}, {}
        self.waiters = {}
        for p in self.others:
            self.next[p] = self.log.last_index() + 1
            self.match[p] = 0
        # A no-op in the new term lets entries of earlier terms commit and
        # lets ReadIndex know the leader's commit index is current.
        noop = Entry(self.term, self.log.last_index() + 1, None)
        try:
            self.log.append([noop])
        except Exception as e:
            self._fatal(e)
            return
        self._advance_commit()
        self._kick_all()
        self.cond.notify_all()

    def _kick_all(self) -> None:
        for ev in self.kick.values():
            ev.set()

    def handle_request_vote(self, req: VoteReq) -> VoteResp:
        with self.mu:
            if self.stopped:
                raise StoppedError()
            if self.failed is not None:
                raise self.failed
            up_to_date = (req.last_log_term > self.log.last_term()
                          or (req.last_log_term == self.log.last_term()
                              and req.last_log_index >= self.log.last_index()))
            if req.pre_vote:
                # Would we vote for this candidate? Never disturb a live leader.
                hearing = (self.role == LEADER
                           or time.monotonic() - self.last_contact < self.cfg.election_timeout)
                return VoteResp(self.term, req.term >= self.term and up_to_date and not hearing)
            if req.term < self.term:
                return VoteResp(self.term)
            if req.term > self.term:
                self._step_down(req.term)
            resp = VoteResp(self.term)
            if (self.voted_for in ("", req.candidate)) and up_to_date:
                self.voted_for = req.candidate
                self._persist_hard_state()
                if self.failed is not None:
                    raise self.failed
                self._reset_election_timer()
                resp.granted = True
            return resp

    # ---- log replication: follower side ----

    def handle_append_entries(self, req: AppendReq) -> AppendResp:
        with self.mu:
            if self.stopped:
                raise StoppedError()
            if self.failed is not None:
                raise self.failed
            if req.term < self.term:
                return AppendResp(self.term)
            if req.term > self.term or self.role != FOLLOWER:
                self._step_down(req.term)
            self.leader, self.last_contact = req.leader, time.monotonic()
            self._reset_election_timer()
            self._abort_restore()
            resp = AppendResp(self.term)
            l = self.log

            if req.prev_index > l.last_index():
                resp.conflict_index = l.last_index() + 1
                return resp
            if req.prev_index >= l.prev_index:  # below that, everything is committed and matches
                t = self._term_of(req.prev_index)
                if t != req.prev_term:
                    resp.conflict_term = t
                    resp.conflict_index = l.first_index_of_term(t)
                    if resp.conflict_index == 0:
                        resp.conflict_index = req.prev_index
                    return resp
            to_append = []
            for i, e in enumerate(req.entries):
                if e.index <= l.prev_index:
                    continue
                if e.index <= l.last_index():
                    if l.term(e.index) == e.term:
                        continue
                    if e.index <= self.commit:
                        err = RuntimeError("raft: leader asks to overwrite committed entry %d" % e.index)
                        self._fatal(err)
                        raise err
                    try:
                        l.truncate_from(e.index)
                    except Exception as ex:
                        self._fatal(ex)
                        raise
                    self._log_truncated(e.index)
                to_append = req.entries[i:]
                break
            try:
                l.append(to_append)
            except Exception as ex:
                self._fatal(ex)
                raise
            for e in to_append:
                if e.kind == KIND_CONFIG:  # takes effect when appended, not when committed
                    self._config_entry_appended(e)
            last_new = req.prev_index + len(req.entries)
            if req.commit > self.commit:
                c = min(req.commit, last_new)
                if c > self.commit:
                    self.commit = c
                    self.cond.notify_all()
            resp.success = True
            return resp

    def _abort_restore(self) -> None:
        if self.restoring is not None:
            if self.restoring.started:
                self.cfg.sm.restore_abort()
            self.restoring = None
            self.cond.notify_all()

    def handle_install_snapshot(self, req: SnapReq) -> SnapResp:
        with self.mu:
            if self.stopped:
                raise StoppedError()
            if self.failed is not None:
                raise self.failed
            if req.term < self.term:
                return SnapResp(self.term)
            if req.term > self.term or self.role != FOLLOWER:
                self._step_down(req.term)
            self.leader, self.last_contact = req.leader, time.monotonic()
            self._reset_election_timer()
            resp = SnapResp(self.term)

            if self.applied >= req.snap_index and self.restoring is None:
                resp.ok, resp.skip = True, True  # already that far; leader continues with entries
                return resp
            if req.seq == 0:
                self._abort_restore()
                s = _RestoreSession(req.session, req.snap_index, req.snap_term)
                self.restoring = s  # pauses the apply loop
                try:
                    self._wait_until(time.monotonic() + 30,
                                     lambda: not self.applying or self.restoring is not s)
                except Exception:
                    return resp
                if self.restoring is not s:
                    return resp
                try:
                    self.cfg.sm.restore_begin()
                except Exception as e:
                    self.restoring = None
                    self._fatal(e)
                    raise
                s.started = True
            s = self.restoring
            if s is None or s.id != req.session or s.next_seq != req.seq or not s.started:
                return resp
            try:
                self.cfg.sm.restore_chunk(req.items)
            except Exception as e:
                self._abort_restore()
                self._fatal(e)
                raise
            s.next_seq += 1
            if req.done:
                mem = Membership.decode(req.config) if req.config else None
                if mem is not None:
                    # Before the swap: a crash after it must find the new configuration.
                    try:
                        save_base(self.cfg.dir, s.snap_idx, mem)
                    except Exception as e:
                        self._abort_restore()
                        self._fatal(e)
                        raise
                try:
                    self.cfg.sm.restore_finish(s.snap_idx, s.snap_term)
                except Exception as e:
                    self.restoring = None
                    self._fatal(e)
                    raise
                try:
                    self.log.reset(s.snap_idx, s.snap_term)
                except Exception as e:
                    self._fatal(e)
                    raise
                if s.snap_idx > self.commit:
                    self.commit = s.snap_idx
                self.applied = self.sm_applied = self.durable = s.snap_idx
                self.restoring = None
                if mem is not None:
                    self.base_index, self.base, self.hist = s.snap_idx, mem, []
                    self._set_config(mem, s.snap_idx)
                self.cfg.logf("[%s] installed snapshot at %d", self.cfg.id, s.snap_idx)
                self.cond.notify_all()
            resp.ok = True
            return resp

    # ---- log replication: leader side ----

    def _advance_commit(self) -> None:
        m = [self.log.last_index()] if self.cfg.id in self.voters else []  # own log is durable
        m.extend(self.match.get(p, 0) for p in self.voters if p != self.cfg.id)
        m.sort(reverse=True)
        q = self._quorum()
        if len(m) < q:
            return
        c = m[q - 1]
        if c > self.commit:
            t = self.log.term(c)
            if t is not None and t == self.term:  # only current-term entries commit by counting
                self.commit = c
                self._notify_committed()
                self.cond.notify_all()

    def _replicator(self, peer: str) -> None:
        ev = self.kick[peer]
        while not self.stop_ev.is_set():
            ev.wait(self.cfg.heartbeat_interval)
            ev.clear()
            if self.stop_ev.is_set() or self.kick.get(peer) is not ev:  # stopped or removed
                return
            while self._replicate_once(peer):
                pass

    def _replicate_once(self, peer: str) -> bool:
        """Send one AppendEntries (or a whole snapshot) to peer and report
        whether more should be sent right away."""
        with self.mu:
            if self.role != LEADER or self.failed is not None or self.stopped:
                return False
            if peer not in self.next:
                return False
            term, nxt = self.term, self.next[peer]
            if nxt <= self.log.prev_index:  # the entry before next was compacted away
                send_snap = True
            else:
                send_snap = False
                prev_index = nxt - 1
                prev_term = self._term_of(prev_index)
                entries = self.log.slice(nxt, self.cfg.max_batch_entries, self.cfg.max_batch_bytes)
                req = AppendReq(term, self.cfg.id, prev_index, prev_term, entries, self.commit)
                sent_at = time.monotonic()
        if send_snap:
            return self._send_snapshot(peer, term)

        try:
            resp = self.cfg.transport.append_entries(peer, req)
        except Exception:
            return False

        with self.mu:
            if resp is None or self.role != LEADER or self.term != term or peer not in self.next:
                return False
            if resp.term > self.term:
                self._step_down(resp.term)
                return False
            if sent_at > self.ack_time.get(peer, float("-inf")):
                self.ack_time[peer] = sent_at
                self.cond.notify_all()
            if resp.success:
                m = prev_index + len(entries)
                if m > self.match.get(peer, 0):
                    self.match[peer] = m
                self.next[peer] = self.match[peer] + 1
                self._advance_commit()
                return self.next[peer] <= self.log.last_index()
            nx = resp.conflict_index
            if resp.conflict_term != 0:
                i = self.log.last_index_of_term(resp.conflict_term)
                if i != 0:
                    nx = i + 1
            if nx < 1:
                nx = 1
            if nx >= nxt:
                nx = nxt - 1
            if nx < 1:
                nx = 1
            self.next[peer] = nx
            return True

    def _send_snapshot(self, peer: str, term: int) -> bool:
        """Stream the state machine to a follower that fell behind the retained
        log. The scan is fuzzy: it runs while new commands keep being applied,
        so the snapshot is anchored at the applied index at the start and the
        log after that index (pinned against compaction) repairs it on replay."""
        with self.mu:
            if self.role != LEADER or self.term != term:
                return False
            snap_index = self.sm_applied
            snap_term = self.log.term(snap_index)
            if snap_term is None:
                return False
            config = self._config_at(snap_index).encode()
            self.snap_pin[peer] = snap_index
            session = self.rnd.getrandbits(64)
        try:
            cursor = None
            seq = 0
            while True:
                items, next_cursor, done = self.cfg.sm.snapshot_scan(cursor, self.cfg.snapshot_chunk_bytes)
                sent_at = time.monotonic()
                try:
                    resp = self.cfg.transport.install_snapshot(peer, SnapReq(
                        term, self.cfg.id, session, seq, snap_index, snap_term,
                        [KV(k, v) for k, v in items], done, config))
                except Exception:
                    return False
                with self.mu:
                    if (resp is None or self.role != LEADER or self.term != term or self.stopped
                            or peer not in self.next):
                        return False
                    if resp.term > self.term:
                        self._step_down(resp.term)
                        return False
                    if sent_at > self.ack_time.get(peer, float("-inf")):
                        self.ack_time[peer] = sent_at
                        self.cond.notify_all()
                    if not resp.ok:
                        return False
                    if done or resp.skip:
                        if snap_index > self.match.get(peer, 0) and not resp.skip:
                            self.match[peer] = snap_index
                        if self.next[peer] <= snap_index:
                            self.next[peer] = snap_index + 1
                        return True
                cursor = next_cursor
                seq += 1
        except Exception as e:
            self.cfg.logf("[%s] snapshot scan: %s", self.cfg.id, e)
            return False
        finally:
            with self.mu:
                self.snap_pin.pop(peer, None)

    # ---- proposals ----

    def propose(self, cmd: bytes, timeout: float) -> None:
        """Replicate cmd and return once it was committed (and will be applied
        in order on this node). After a timeout or NotLeaderError the command
        may still end up committed, so only idempotent commands are safe to
        retry blindly."""
        if len(cmd) == 0:
            raise ValueError("raft: empty command")
        if len(cmd) > self.cfg.max_command_bytes:
            raise TooBigError()
        with self.mu:
            if self.stopped:
                raise StoppedError()
            if self.failed is not None:
                raise self.failed
            if self.role != LEADER:
                raise self._not_leader()
        p = _Proposal(bytes(cmd))
        with self.prop_mu:
            self.prop_q.append(p)
        self.prop_sig.set()
        if not p.wait(timeout):
            raise RaftTimeoutError()
        if p.err is not None:
            raise p.err

    def _fail_queued(self, err) -> None:
        with self.prop_mu:
            q, self.prop_q = self.prop_q, []
        for p in q:
            p.complete(err)

    def _append_loop(self) -> None:
        """Turn queued proposals into log entries: everything queued while the
        previous batch was being fsynced goes out with a single fsync."""
        while True:
            self.prop_sig.wait()
            if self.stop_ev.is_set():
                return
            self.prop_sig.clear()
            with self.prop_mu:
                q, self.prop_q = self.prop_q, []
            if not q:
                continue
            with self.mu:
                if self.role != LEADER or self.failed is not None:
                    err = self.failed if self.failed is not None else self._not_leader()
                    fail = err
                else:
                    fail = None
                    entries = []
                    idx = self.log.last_index()
                    for p in q:
                        idx += 1
                        p.term, p.index = self.term, idx
                        entries.append(Entry(self.term, idx, p.data))
                    try:
                        self.log.append(entries)
                    except Exception as e:
                        self._fatal(e)
                        fail = e
                    else:
                        for p in q:
                            self.waiters[p.index] = p
                        self._advance_commit()  # single-node clusters commit here
                        self._kick_all()
            if fail is not None:
                for p in q:
                    p.complete(fail)

    # ---- applying ----

    def _apply_loop(self) -> None:
        self.mu.acquire()
        try:
            while True:
                while (not self.stopped and self.failed is None
                       and not (self.commit > self.applied and self.restoring is None)):
                    self.cond.wait()
                if self.stopped or self.failed is not None:
                    return
                lo, hi = self.applied + 1, self.commit
                if hi - lo >= MAX_APPLY_BATCH:
                    hi = lo + MAX_APPLY_BATCH - 1
                cmds = [e.data for e in self.log.slice(lo, hi - lo + 1, 1 << 62)
                        if e.data is not None and e.kind == KIND_NORMAL]
                term = self._term_of(hi)
                self.applying = True
                self.mu.release()
                err = None
                try:
                    if cmds:
                        self.cfg.sm.apply(hi, term, cmds)
                except Exception as e:
                    err = e
                finally:
                    self.mu.acquire()
                self.applying = False
                if err is not None:
                    self._fatal(RuntimeError("raft: apply: %s" % err))
                    return
                if self.restoring is not None or self.applied >= hi:  # a snapshot replaced the state meanwhile
                    self.cond.notify_all()
                    continue
                self.applied = hi
                if cmds:
                    self.sm_applied = hi
                self._maybe_compact()
                self.cond.notify_all()
        finally:
            self.mu.release()

    def _notify_committed(self) -> None:
        """Complete proposals whose entries are now committed. The caller is
        acknowledged at commit, not at apply: the entry is durable on a
        majority and can no longer be lost. Reads wait for the apply in
        read_barrier, so they still observe it."""
        for idx in list(self.waiters):
            if idx > self.commit:
                continue
            p = self.waiters.pop(idx)
            t = self.log.term(idx)
            if t is not None and t == p.term:
                p.complete(None)
            else:
                p.complete(self._not_leader())

    def _maybe_compact(self) -> None:
        target = self.durable
        if target <= self.cfg.log_retain:
            return
        target -= self.cfg.log_retain
        for pin in self.snap_pin.values():
            if pin < target:
                target = pin
        if target > self.log.prev_index:
            if not self._fold_base(target):
                return
            try:
                self.log.compact_to(target)
            except Exception as e:
                self._fatal(e)

    def _sync_loop(self) -> None:
        """Periodically make the state machine durable so the log can be
        compacted up to what is safely on disk."""
        while not self.stop_ev.wait(0.05):
            with self.mu:
                need = self.failed is None and self.restoring is None and self.sm_applied > self.durable
            if not need:
                continue
            err = None
            idx = 0
            try:
                idx = self.cfg.sm.sync()
            except Exception as e:
                err = e
            with self.mu:
                if err is not None:
                    if not self.stopped:
                        self._fatal(RuntimeError("raft: state machine sync: %s" % err))
                elif idx > self.durable and self.restoring is None and idx <= self.sm_applied:
                    self.durable = idx
                    self._maybe_compact()

    # ---- linearizable reads ----

    def read_barrier(self, timeout: float) -> None:
        """Return once this node, still being the leader, has applied
        everything committed before the call (the ReadIndex protocol). Reads
        served from the state machine afterwards are linearizable."""
        deadline = time.monotonic() + timeout
        with self.mu:
            if self.role != LEADER:
                raise self._not_leader()
            term = self.term
            # The commit index is only trustworthy once an entry of this term committed.
            self._wait_until(deadline, lambda: (self.role != LEADER or self.term != term
                                                or self.log.term(self.commit) == term))
            if self.role != LEADER or self.term != term:
                raise self._not_leader()
            read_idx = self.commit
            t0 = time.monotonic()
            self._kick_all()

            def confirmed() -> bool:
                c = 1 if self.cfg.id in self.voters else 0
                for p in self.voters:
                    if p != self.cfg.id and self.ack_time.get(p, float("-inf")) >= t0:
                        c += 1
                return c >= self._quorum()

            self._wait_until(deadline, lambda: (self.role != LEADER or self.term != term
                                                or (confirmed() and self.applied >= read_idx)))
            if self.role != LEADER or self.term != term:
                raise self._not_leader()

    # ---- membership changes ----

    def _can_change_config(self) -> bool:
        """One change at a time, and only once an entry of this term has
        committed (so the previous configuration is known to be committed).
        mu held, leader."""
        return self.config_index <= self.commit and self.log.term(self.commit) == self.term

    def _append_config(self, m: Membership) -> None:
        idx = self.log.last_index() + 1
        e = Entry(self.term, idx, m.encode(), KIND_CONFIG)
        try:
            self.log.append([e])
        except Exception as ex:
            self._fatal(ex)
            raise
        self.hist.append((idx, m))
        self._set_config(m, idx)  # before advancing the commit: the quorum changed
        self._advance_commit()
        self._kick_all()

    def add_member(self, nid: str, info: str = "") -> bool:
        """Leader only: add nid as a non-voting learner. It receives the log or
        a snapshot like any follower and is promoted to voter automatically
        once it has caught up. Returns False if nid is already a member or
        another change is still in flight (the caller may retry)."""
        with self.mu:
            if self.stopped:
                raise StoppedError()
            if self.failed is not None:
                raise self.failed
            if self.role != LEADER:
                raise self._not_leader()
            if nid in self.mem.members or not self._can_change_config():
                return False
            self._append_config(self.mem.with_learner(nid, info))
            return True

    def remove_member(self, nid: str) -> bool:
        """Leader only: drop nid (voter or learner) from the configuration.
        Safe only if the caller established that nid is really gone, since
        the quorum shrinks as soon as the entry is appended. Returns False if
        nid is not a member or another change is in flight."""
        with self.mu:
            if self.stopped:
                raise StoppedError()
            if self.failed is not None:
                raise self.failed
            if self.role != LEADER:
                raise self._not_leader()
            if nid == self.cfg.id:
                raise RaftError("raft: a leader cannot remove itself")
            if nid not in self.mem.members or not self._can_change_config():
                return False
            self._append_config(self.mem.without(nid))
            return True

    def _promote_loop(self) -> None:
        """Promote caught-up learners to voters, one at a time."""
        while not self.stop_ev.wait(self.cfg.heartbeat_interval * 2):
            with self.mu:
                if self.role != LEADER or self.failed is not None or not self._can_change_config():
                    continue
                now, last = time.monotonic(), self.log.last_index()
                for p in self.mem.learners():
                    m = self.match.get(p, 0)
                    fresh = now - self.ack_time.get(p, float("-inf")) < self.cfg.election_timeout
                    if m > 0 and last - m <= self.cfg.max_batch_entries and fresh:
                        self.cfg.logf("[%s] promoting %s to voter", self.cfg.id, p)
                        try:
                            self._append_config(self.mem.with_voter(p))
                        except Exception:
                            pass
                        break

    # ---- bootstrapping a new node from a peer's state ----

    def handle_pull_snapshot(self, req: PullReq) -> PullResp:
        """Serve one chunk of this node's state to a joining node. Any member
        may answer; the copy is a fuzzy snapshot anchored at the applied index
        of the first chunk, completed by normal replication afterwards."""
        resp = PullResp()
        with self.mu:
            if self.stopped:
                raise StoppedError()
            if self.failed is not None:
                raise self.failed
            if self.restoring is not None:
                raise RaftError("raft: busy installing a snapshot")
            if req.cursor is None:
                resp.snap_index = self.sm_applied
                resp.snap_term = self.log.term(resp.snap_index)
                if resp.snap_term is None:
                    raise RaftError("raft: snapshot anchor is not available")
                resp.config = self._config_at(resp.snap_index).encode()
        items, nxt, done = self.cfg.sm.snapshot_scan(req.cursor, self.cfg.snapshot_chunk_bytes)
        resp.items = [KV(k, v) for k, v in items]
        resp.next_cursor, resp.done = nxt, done
        return resp

    def bootstrap(self, pull: Callable[[PullReq], PullResp]) -> bool:
        """Initialise a fresh, not yet started node with a copy of a peer's
        state, fetched chunk by chunk through pull. Returns False if there was
        nothing to copy or the node is not fresh. On failure the node is left
        empty, and the leader will send it a snapshot instead."""
        with self.mu:
            if (self.started or self.applied != 0 or self.log.last_index() != 0
                    or self.log.prev_index != 0):
                return False
        sm = self.cfg.sm
        anchor = None
        cursor = None
        sm.restore_begin()
        try:
            while True:
                r = pull(PullReq(cursor))
                if anchor is None:
                    if r.snap_index == 0:  # the peer has applied nothing: nothing to copy
                        sm.restore_abort()
                        return False
                    anchor = (r.snap_index, r.snap_term, Membership.decode(r.config))
                sm.restore_chunk(r.items)
                if r.done:
                    break
                cursor = r.next_cursor
            idx, term, mem = anchor
            with self.mu:
                save_base(self.cfg.dir, idx, mem)
                sm.restore_finish(idx, term)
                self.log.reset(idx, term)
                self.commit = self.applied = self.sm_applied = self.durable = idx
                self.base_index, self.base, self.hist = idx, mem, []
                self._set_config(mem, idx)
            self.cfg.logf("[%s] bootstrapped from a peer at %d", self.cfg.id, idx)
            return True
        except BaseException:
            sm.restore_abort()
            raise
