"""Connects the Raft node to the storage engine: a state machine that applies
replicated commands to an engine.DB, and (see store.py) a kvnet store that
routes writes through consensus and reads through ReadIndex."""

import os
import shutil
import struct

from ..engine import DB, NotFoundError, Op, Options
from ..raft.types import KV, StateMachine
from ..util import AtomicInt, RWLock, VarintError, append_uvarint, read_uvarint, sync_dir

# Reserved key holding (index, term) of the last applied command. It is
# written in the same atomic batch as the command's effects, so the data and
# the applied index can never disagree after a crash.
RESERVED_PREFIX = b"\x00raft/"
APPLIED_KEY = b"\x00raft/applied"

_APPLIED = struct.Struct("<QQ")


def is_reserved(key: bytes) -> bool:
    return key.startswith(RESERVED_PREFIX)


class BadCommandError(Exception):
    def __init__(self):
        super().__init__("replica: malformed command")


# ---- command encoding ----


def encode_command(ops) -> bytes:
    """Serialise a batch of operations as one replicated command."""
    b = bytearray()
    append_uvarint(b, len(ops))
    for o in ops:
        b.append(0 if o.delete else 1)
        append_uvarint(b, len(o.key))
        b += o.key
        append_uvarint(b, len(o.value))
        b += o.value
    return bytes(b)


def decode_command(b: bytes):
    try:
        n, pos = read_uvarint(b, 0)
        if n > len(b):
            raise BadCommandError()
        ops = []
        for _ in range(n):
            if pos >= len(b):
                raise BadCommandError()
            kind = b[pos]
            pos += 1
            parts = []
            for _ in range(2):
                ln, pos = read_uvarint(b, pos)
                if ln > len(b) - pos:
                    raise BadCommandError()
                parts.append(bytes(b[pos:pos + ln]))
                pos += ln
            ops.append(Op(parts[0], parts[1], kind == 0))
        return ops
    except VarintError:
        raise BadCommandError() from None


def encode_applied(index: int, term: int) -> bytes:
    return _APPLIED.pack(index, term)


class KVMachine(StateMachine):
    """raft.StateMachine on top of an engine.DB.

    Layout under dir:  db/ (live data), db.restore/ (snapshot being installed),
    restore.ready (commit point of a snapshot install).
    """

    def __init__(self, directory: str, opts: Options = None):
        self.dir = directory
        self.opts = Options(**vars(opts)) if opts is not None else Options()
        # Durability of applied commands comes from the Raft log, which keeps
        # them until sync() reports them durable; fsync-ing every apply would
        # only add latency to the write path.
        self.opts.no_sync = True
        self.mu = RWLock()  # guards db (swapped by snapshot installs)
        self.db = None
        self.staging = None
        self._applied = AtomicInt(0)  # index of the last command applied (maybe not yet durable)

        os.makedirs(directory, exist_ok=True)
        # Finish or roll back an interrupted snapshot install.
        ready = os.path.join(directory, "restore.ready")
        staged = os.path.join(directory, "db.restore")
        live = os.path.join(directory, "db")
        if os.path.exists(ready):
            if os.path.exists(staged):
                shutil.rmtree(live, ignore_errors=True)
                os.rename(staged, live)
            os.remove(ready)
        else:
            shutil.rmtree(staged, ignore_errors=True)
        self.db = DB.open(live, self.opts)
        try:
            idx, _ = self.applied()
        except BaseException:
            self.db.close()
            raise
        self._applied.store(idx)

    @classmethod
    def open(cls, directory: str, opts: Options = None) -> "KVMachine":
        return cls(directory, opts)

    def close(self) -> None:
        with self.mu.write():
            if self.staging is not None:
                self.staging.close()
                self.staging = None
            self.db.close()

    # ---- raft.StateMachine ----

    def applied(self):
        with self.mu.read():
            try:
                v = self.db.read(APPLIED_KEY)
            except NotFoundError:
                return 0, 0
        if len(v) != 16:
            raise RuntimeError("replica: bad applied marker")
        return _APPLIED.unpack(v)

    def apply(self, index: int, term: int, cmds) -> None:
        ops = []
        for c in cmds:
            ops.extend(decode_command(c))
        ops.append(Op(APPLIED_KEY, encode_applied(index, term)))
        with self.mu.read():
            self.db.write(ops)
        self._applied.store(index)

    def sync(self) -> int:
        """fsync the engine and return the index that is durable afterwards."""
        with self.mu.read():
            idx = self._applied.load()  # read first: anything applied later is not covered
            self.db.sync()
        return idx

    def snapshot_scan(self, cursor, max_bytes: int):
        """Page through user data (the reserved marker is skipped)."""
        with self.mu.read():
            kvs, more = self.db.scan(cursor, max_bytes)
        items = [KV(kv.key, kv.value) for kv in kvs if not is_reserved(kv.key)]
        nxt = kvs[-1].key + b"\x00" if more else None
        return items, nxt, not more

    def restore_begin(self) -> None:
        self.restore_abort()
        staged = os.path.join(self.dir, "db.restore")
        shutil.rmtree(staged, ignore_errors=True)
        # Default sync: every chunk is fsynced, so "ready" implies durable.
        db = DB.open(staged, Options(block_cache_size=-1, compaction_trigger=-1))
        with self.mu.write():
            self.staging = db

    def restore_chunk(self, items) -> None:
        if not items:
            return
        ops = [Op(kv.key, kv.value) for kv in items]
        with self.mu.read():
            st = self.staging
        if st is None:
            raise RuntimeError("replica: no restore in progress")
        st.write(ops)

    def restore_finish(self, index: int, term: int) -> None:
        with self.mu.write():
            if self.staging is None:
                raise RuntimeError("replica: no restore in progress")
            self.staging.write([Op(APPLIED_KEY, encode_applied(index, term))])
            st, self.staging = self.staging, None
            st.close()

            # Commit point: once restore.ready exists, KVMachine() completes the swap.
            ready = os.path.join(self.dir, "restore.ready")
            fd = os.open(ready, os.O_CREAT | os.O_WRONLY, 0o644)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            sync_dir(self.dir)
            self.db.close()
            live = os.path.join(self.dir, "db")
            staged = os.path.join(self.dir, "db.restore")
            shutil.rmtree(live)
            os.rename(staged, live)
            os.remove(ready)
            sync_dir(self.dir)
            self.db = DB.open(live, self.opts)
            self._applied.store(index)

    def restore_abort(self) -> None:
        with self.mu.write():
            st, self.staging = self.staging, None
        if st is not None:
            st.close()
            shutil.rmtree(os.path.join(self.dir, "db.restore"), ignore_errors=True)

    # ---- reads (callers must have passed raft.Node.read_barrier) ----

    def get(self, key: bytes) -> bytes:
        with self.mu.read():
            return self.db.read(key)

    def range(self, start=None, end=None):
        with self.mu.read():
            kvs = self.db.read_key_range(start, end)
        return [kv for kv in kvs if not is_reserved(kv.key)]
