"""A persistent, sorted key/value storage engine built as an LSM tree:
WAL -> memtable -> immutable SSTables organised in levels, with a manifest
tying the files together, leveled background compaction with bounded-size
merges, group commit and a block cache."""

import os
import threading
import time
from dataclasses import dataclass
from typing import NamedTuple

from .cache import new_block_cache
from .codec import KIND_DELETE, KIND_PUT, Entry
from .compaction import DEFAULT_LEVEL_BASE, DEFAULT_TARGET_FILE_SIZE, CompactionMixin
from .errors import ClosedError, EmptyKeyError, NotFoundError
from .manifest import NUM_LEVELS, MANIFEST_NAME, read_manifest, write_manifest
from .memtable import Memtable
from .merge import merge_entries
from .sstable import open_sstable, sst_path, write_table
from .stats import Counters, LevelStats, Stats
from .throttle import Throttle, ThrottleMixin
from .version import Version
from .wal import create_wal, replay_wal, wal_path
from ..util import Signal


@dataclass
class Options:
    # memtable_size is the approximate size at which the memtable is flushed
    # to an SSTable. Default 4 MiB.
    memtable_size: int = 0
    # no_sync disables fsync on every write. Faster, but a power loss may drop
    # recent writes. Default (False) = every acknowledged write is durable.
    no_sync: bool = False
    # block_cache_size is the capacity in bytes of the decoded-block LRU cache.
    # Default 32 MiB; negative disables the cache.
    block_cache_size: int = 0
    # compaction_trigger is the number of level-0 files that triggers a
    # compaction into level 1. Writes are slowed down at twice this number and
    # stopped at three times it. Default 4; negative disables compaction.
    compaction_trigger: int = 0
    # target_file_size is the approximate size of SSTables produced by
    # compaction. Default 8 MiB.
    target_file_size: int = 0
    # level_base_bytes is the capacity of level 1; each deeper level holds 10x
    # more. Default 64 MiB.
    level_base_bytes: int = 0


class KV(NamedTuple):
    key: bytes
    value: bytes


class Op(NamedTuple):
    """One element of an atomic mixed batch."""

    key: bytes
    value: bytes = b""
    delete: bool = False


class _WriteReq:
    __slots__ = ("batch", "err", "finished", "promoted", "wake")

    def __init__(self, batch):
        self.batch = batch
        self.err = None
        self.finished = False  # set (before wake) when a leader committed the batch
        self.promoted = False  # set (before wake) when this writer must lead the next round
        self.wake = threading.Event()


class Snapshot:
    """A pinned read view: the memtables plus a referenced table layout."""

    def __init__(self, mem, imm, v):
        self.mem, self.imm, self.v = mem, imm, v

    def release(self) -> None:
        self.v.unref()


class DB(CompactionMixin, ThrottleMixin):
    def __init__(self, directory: str, opts: Options = None):
        self.dir = directory
        self.opts = Options(**vars(opts)) if opts is not None else Options()
        o = self.opts
        if o.memtable_size <= 0:
            o.memtable_size = 4 << 20
        if o.block_cache_size == 0:
            o.block_cache_size = 32 << 20
        if o.compaction_trigger == 0:
            o.compaction_trigger = 4
        if o.target_file_size <= 0:
            o.target_file_size = DEFAULT_TARGET_FILE_SIZE
        if o.level_base_bytes <= 0:
            o.level_base_bytes = DEFAULT_LEVEL_BASE

        self.write_mu = threading.Lock()  # held while a commit group is applied; also by close()
        self.qmu = threading.Lock()  # group commit queue
        self.queue = []
        self.leading = False

        self.cache = new_block_cache(o.block_cache_size)

        self.mu = threading.Lock()  # guards the fields below
        self.cond = threading.Condition(self.mu)  # signalled when imm is flushed or on failure
        self.mem = None
        self.imm = None  # being flushed, read-only
        self.wal = None
        self.imm_wal = None
        self.cur = None  # current table layout (Version)
        self.man = None
        self.next_num = 0
        self.closed = False
        self.failed = None

        self.ctr = Counters()
        self.thr = Throttle()
        self.quit = threading.Event()  # set by close(); stops auxiliary threads
        self.compact_mu = threading.Lock()  # one compaction (or compact_all) at a time
        self.compact_ptr = [None] * NUM_LEVELS

        self.flush_sig = Signal()
        self.compact_sig = Signal()
        self._threads = {}

        os.makedirs(directory, exist_ok=True)
        try:
            self._recover()
        except BaseException:
            self._close_files()
            raise
        self._start("flush", self._flush_loop)
        self._start("compact", self._compact_loop)
        self._start("throttle", self._throttle_loop)
        self._kick_compaction()

    @classmethod
    def open(cls, directory: str, opts: Options = None) -> "DB":
        return cls(directory, opts)

    def _start(self, name: str, fn) -> None:
        t = threading.Thread(target=fn, name="engine-" + name, daemon=True)
        self._threads[name] = t
        t.start()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        try:
            self.close()
        except ClosedError:
            pass

    # ---- recovery ----

    def _recover(self) -> None:
        man = read_manifest(self.dir)
        in_manifest = set()
        max_num = man.log_num
        for ids in man.levels:
            for fid in ids:
                in_manifest.add(fid)
                max_num = max(max_num, fid)
        wal_nums = []
        for name in os.listdir(self.dir):
            if name == MANIFEST_NAME + ".tmp":
                os.remove(os.path.join(self.dir, name))
            elif name.endswith(".sst"):
                try:
                    fid = int(name[:-4])
                except ValueError:
                    continue
                max_num = max(max_num, fid)
                if fid not in in_manifest:  # orphan from a flush that crashed before the manifest update
                    os.remove(os.path.join(self.dir, name))
            elif name.startswith("wal-") and name.endswith(".log"):
                try:
                    n = int(name[4:-4])
                except ValueError:
                    continue
                max_num = max(max_num, n)
                wal_nums.append(n)

        levels = [[] for _ in range(NUM_LEVELS)]

        def release_opened():
            for files in levels:
                for t in files:
                    t.unref()

        try:
            for l, ids in enumerate(man.levels):
                for fid in ids:
                    levels[l].append(open_sstable(sst_path(self.dir, fid), fid, self.cache))
                if l > 0:
                    levels[l].sort(key=lambda t: t.smallest)
        except BaseException:
            release_opened()
            raise

        # Replay WALs that were not yet flushed.
        wal_nums.sort()
        replayed = Memtable()

        def apply(batch):
            for e in batch:
                replayed.put(e.key, e.value, e.kind)

        for n in wal_nums:
            if n < man.log_num:
                continue
            replay_wal(wal_path(self.dir, n), apply)

        wal_num = max_num + 1
        self.next_num = wal_num + 1
        if len(replayed) > 0:
            # Persist recovered data right away so old WALs can be dropped.
            fid = self.next_num
            self.next_num += 1
            try:
                t = write_table(self.dir, fid, replayed.iter(None), self.cache)
            except BaseException:
                release_opened()
                raise
            levels[0].append(t)
        self.cur = Version(levels)
        release_opened()  # the version now holds its own references
        man = self.cur.manifest(wal_num)
        write_manifest(self.dir, man)
        self.man = man
        for n in wal_nums:
            os.remove(wal_path(self.dir, n))
        self.wal = create_wal(self.dir, wal_num, not self.opts.no_sync)
        self.mem = Memtable()

    # ---- write path ----

    def put(self, key: bytes, value: bytes) -> None:
        self.batch_put([key], [value])

    def batch_put(self, keys, values) -> None:
        """Apply all pairs atomically (one WAL record) and durably."""
        if len(keys) != len(values):
            raise ValueError("engine: %d keys but %d values" % (len(keys), len(values)))
        batch = []
        for k, v in zip(keys, values):
            if len(k) == 0:
                raise EmptyKeyError()
            batch.append(Entry(bytes(k), bytes(v), KIND_PUT))
        self._write(batch)

    def write(self, ops) -> None:
        """Apply puts and deletes atomically (one WAL record)."""
        batch = []
        for o in ops:
            if len(o.key) == 0:
                raise EmptyKeyError()
            if o.delete:
                batch.append(Entry(bytes(o.key), b"", KIND_DELETE))
            else:
                batch.append(Entry(bytes(o.key), bytes(o.value), KIND_PUT))
        self._write(batch)

    def delete(self, key: bytes) -> None:
        """Write a tombstone. Deleting a missing key is not an error."""
        if len(key) == 0:
            raise EmptyKeyError()
        self._write([Entry(bytes(key), b"", KIND_DELETE)])

    def _write(self, batch) -> None:
        """Group commit. Concurrent writers queue their batch; the first one
        becomes the leader and commits everything queued so far with a single
        WAL write and a single fsync, then wakes the others. While the leader
        is in fsync, new writers accumulate for the next round.

        A leader runs exactly one round (which includes its own batch) and
        then hands leadership to the oldest queued writer. Looping until the
        queue is empty would starve the leader's own caller under continuous
        load."""
        if not batch:
            return
        req = _WriteReq(batch)
        with self.qmu:
            self.queue.append(req)
            follower = self.leading
            if not follower:
                self.leading = True
        if follower:
            req.wake.wait()
            if req.finished:  # committed by another leader
                if req.err is not None:
                    raise req.err
                return
            # promoted: our batch is still queued

        with self.qmu:
            reqs = self.queue
            self.queue = []
        err = None
        try:
            self._commit(reqs)
        except BaseException as e:
            err = e
        for r in reqs:
            r.err = err
            r.finished = True
            r.wake.set()

        with self.qmu:
            if self.queue:
                nxt = self.queue[0]  # leadership stays "taken" and moves to nxt
                nxt.promoted = True
                nxt.wake.set()
            else:
                self.leading = False
        if req.err is not None:
            raise req.err

    def _mem_full(self) -> bool:
        return self.mem.approx_size() >= self.opts.memtable_size

    def _stalled(self) -> bool:
        """Compaction is too far behind. Caller holds self.mu."""
        return self._compaction_enabled() and len(self.cur.levels[0]) >= 3 * self.opts.compaction_trigger

    def _commit(self, reqs) -> None:
        with self.write_mu:
            # Adaptive brake: admit bytes at the rate compaction can sustain, so
            # the hard stall below is a last resort rather than the normal regulator.
            size = 0
            for r in reqs:
                for e in r.batch:
                    size += len(e.key) + len(e.value)
            d = self.thr.wait(size)
            if d > 0:
                self.ctr.add("slowdowns")
                self.ctr.add("slowdown_nanos", int(d * 1e9))

            # Backpressure keeps latency predictable: when the memtable is full,
            # wait for the previous flush and for compaction to catch up rather
            # than let memory or the number of files (read cost) grow without bound.
            with self.mu:
                def must_wait():
                    return (not self.closed and self.failed is None and self._mem_full()
                            and (self.imm is not None or self._stalled()))

                if must_wait():
                    t0 = time.monotonic()
                    while must_wait():
                        self.cond.wait()
                    self.ctr.add("stalls")
                    self.ctr.add("stall_nanos", int((time.monotonic() - t0) * 1e9))
                if self.closed:
                    raise ClosedError()
                if self.failed is not None:
                    raise self.failed

            if self._mem_full():
                try:
                    self._rotate()
                except BaseException as e:
                    raise self.fail(e)
            batches = [r.batch for r in reqs]
            try:
                self.wal.append(batches)
            except BaseException as e:
                # A partial record may sit in the log; refuse further writes.
                raise self.fail(e)
            mem = self.mem
            for b in batches:
                for e in b:
                    mem.put(e.key, e.value, e.kind)
            self.ctr.add("written_bytes", size)

    def fail(self, err: BaseException) -> BaseException:
        """Record a fatal error: the DB refuses further writes. Returns err."""
        with self.mu:
            if self.failed is None:
                self.failed = err
            self.cond.notify_all()
        return err

    def _rotate(self) -> None:
        """Freeze the memtable and start a fresh one with its own WAL. Caller
        holds write_mu and has verified imm is None."""
        with self.mu:
            num = self.next_num
            self.next_num += 1
        w = create_wal(self.dir, num, not self.opts.no_sync)
        with self.mu:
            old = self.wal
            self.imm, self.imm_wal = self.mem, old
            self.mem, self.wal = Memtable(), w
        self.flush_sig.kick()

    def _kick_compaction(self) -> None:
        self.compact_sig.kick()

    def _flush_loop(self) -> None:
        while self.flush_sig.wait():
            try:
                self._flush_imm()
            except BaseException as e:
                self.fail(e)

    def _flush_imm(self) -> None:
        with self.mu:
            imm, old_wal = self.imm, self.imm_wal
            if imm is None:
                return
            fid = self.next_num
            self.next_num += 1
            live_wal = self.wal.num  # WAL paired with the current memtable

        t = write_table(self.dir, fid, imm.iter(None), self.cache)
        with self.mu:
            add = [[] for _ in range(NUM_LEVELS)]
            add[0] = [t]
            self._install_version(self.cur.with_changes(None, add), live_wal, [t], None, False)
            self.imm, self.imm_wal = None, None
            self.cond.notify_all()
        self.ctr.add("flushes")
        self.ctr.add("flush_bytes", t.size)
        self._kick_compaction()

        old_wal.close()
        try:
            os.remove(wal_path(self.dir, old_wal.num))
        except OSError:
            pass

    # ---- read path ----

    def snapshot(self) -> Snapshot:
        with self.mu:
            if self.closed:
                raise ClosedError()
            self.cur.ref()
            return Snapshot(self.mem, self.imm, self.cur)

    def read(self, key: bytes) -> bytes:
        """Return the value for key or raise NotFoundError."""
        s = self.snapshot()
        try:
            e = s.mem.get(key)
            if e is None and s.imm is not None:
                e = s.imm.get(key)
            if e is None:
                e = s.v.get(key)
        finally:
            s.release()
        if e is None or e.kind == KIND_DELETE:
            raise NotFoundError()
        return e.value

    def read_key_range(self, start=None, end=None):
        """All live pairs with start <= key <= end in key order. A None start
        means "from the first key"; a None end means "to the last key"."""
        kvs, _ = self._scan(start, end, 0)
        return kvs

    def scan(self, start=None, max_bytes: int = 0):
        """Live pairs with key >= start in key order, stopping once about
        max_bytes of keys+values were collected (at least one pair is returned
        if any exist). Returns (pairs, more); continue from lastKey+b"\\x00".
        It lets callers stream a dataset larger than RAM."""
        return self._scan(start, None, max_bytes)

    def _scan(self, start, end, max_bytes):
        s = self.snapshot()
        try:
            srcs = [s.mem.iter(start, end)]
            if s.imm is not None:
                srcs.append(s.imm.iter(start, end))
            srcs.extend(s.v.iterators(start, True))
            out = []
            size = 0
            for e in merge_entries(srcs):
                if end is not None and e.key > end:
                    break
                if e.kind != KIND_PUT:
                    continue
                if max_bytes > 0 and out and size >= max_bytes:
                    return out, True
                out.append(KV(e.key, e.value))
                size += len(e.key) + len(e.value)
            return out, False
        finally:
            s.release()

    def sync(self) -> None:
        """Make everything written so far durable. Only needed with
        Options.no_sync, where the caller batches durability itself (as the
        replicated state machine does: its durability comes from the Raft log)."""
        with self.mu:
            if self.closed:
                raise ClosedError()
            w, iw = self.wal, self.imm_wal
        for x in (iw, w):  # the frozen WAL first: it holds the older data
            if x is not None:
                x.sync()  # a WAL closed meanwhile was flushed to an SSTable, which is already durable

    def flush(self) -> None:
        """Write the memtable to an SSTable and return when it is durable in
        the table layout (the WAL alone already guarantees durability; this is
        for maintenance such as compact_all)."""
        with self.write_mu:
            with self.mu:
                while not self.closed and self.failed is None and self.imm is not None:
                    self.cond.wait()
                if self.closed:
                    raise ClosedError()
                if self.failed is not None:
                    raise self.failed
                empty = len(self.mem) == 0
            if empty:
                return
            try:
                self._rotate()
            except BaseException as e:
                raise self.fail(e)
            with self.mu:
                while not self.closed and self.failed is None and self.imm is not None:
                    self.cond.wait()
                if self.failed is not None:
                    raise self.failed

    # ---- statistics ----

    def stats(self) -> Stats:
        s = Stats()
        with self.mu:
            for l, files in enumerate(self.cur.levels):
                s.levels[l] = LevelStats(len(files), self.cur.level_bytes(l))
            s.memtable_bytes = self.mem.approx_size()
        s.throttle_rate = self.thr.get()
        c = self.ctr
        s.flushes, s.flush_bytes = c.get("flushes"), c.get("flush_bytes")
        s.compactions, s.trivial_moves = c.get("compactions"), c.get("trivial_moves")
        s.compaction_bytes_read = c.get("compact_bytes_read")
        s.compaction_bytes_written = c.get("compact_bytes_write")
        s.compaction_time = c.get("compact_nanos") / 1e9
        s.slowdowns, s.slowdown_time = c.get("slowdowns"), c.get("slowdown_nanos") / 1e9
        s.stalls, s.stall_time = c.get("stalls"), c.get("stall_nanos") / 1e9
        return s

    # ---- lifecycle ----

    def close(self) -> None:
        """Wait for a pending flush and release all files. Unflushed memtable
        data stays in the WAL and is recovered on the next open."""
        with self.write_mu:
            with self.mu:
                if self.closed:
                    raise ClosedError()
                self.closed = True
                self.cond.notify_all()
            self.quit.set()
            self._threads["throttle"].join()
            self.flush_sig.close()
            self._threads["flush"].join()
            self.compact_sig.close()
            self._threads["compact"].join()
            self._close_files()

    def _close_files(self) -> None:
        if self.wal is not None:
            self.wal.close()
        if self.cur is not None:
            self.cur.unref()
