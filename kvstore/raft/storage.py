import os
import struct
import zlib

from ..util import pread_exact, pwrite_all, sync_dir, write_all, write_file_atomic
from .types import KIND_CONFIG, KIND_NORMAL, Entry

# ---- hard state: current_term and voted_for ----
# Must be durable before answering any RPC that depends on it.


def save_hard_state(directory: str, term: int, voted_for: str) -> None:
    data = ("term %d\nvote %s\n" % (term, voted_for)).encode()
    write_file_atomic(os.path.join(directory, "state"), data, directory)


def load_hard_state(directory: str):
    """Returns (term, voted_for); (0, "") for a fresh directory."""
    try:
        with open(os.path.join(directory, "state"), "rb") as f:
            data = f.read().decode()
    except FileNotFoundError:
        return 0, ""
    term, vote = 0, ""
    for line in data.strip().split("\n"):
        k, _, v = line.partition(" ")
        if k == "term":
            try:
                term = int(v)
            except ValueError as e:
                raise ValueError("raft state: %s" % e) from None
        elif k == "vote":
            vote = v
    return term, vote


# ---- log ----
#
# File: header | record*
#   header: prev_index(u64) prev_term(u64) crc32(u32)   (entries start after prev_index)
#   record: len(u32) crc32(u32) | term(u64) index(u64) data
#           (the top bit of len marks a configuration entry; older logs never set it)
#
# Records are only appended; a conflicting suffix is removed by truncating the
# file. Compaction drops a prefix in memory at once and rewrites the file
# (temp file + rename) once enough garbage has built up.

LOG_HEADER_SIZE = 20
_HDR = struct.Struct("<QQ")
_U32 = struct.Struct("<I")
_REC = struct.Struct("<II")
_TI = struct.Struct("<QQ")
_CONFIG_FLAG = 0x80000000


def log_path(directory: str) -> str:
    return os.path.join(directory, "raft.log")


def encode_header(prev_index: int, prev_term: int) -> bytes:
    h = _HDR.pack(prev_index, prev_term)
    return h + _U32.pack(zlib.crc32(h))


def encode_record(e: Entry) -> bytes:
    payload = _TI.pack(e.term, e.index) + (e.data or b"")
    n = len(payload) | (_CONFIG_FLAG if e.kind == KIND_CONFIG else 0)
    return _REC.pack(n, zlib.crc32(payload)) + payload


class LogStore:
    """The durable Raft log with an in-memory window of entries.

    Not thread-safe: the node serialises access with its own mutex."""

    def __init__(self, directory: str, no_sync: bool):
        self.dir = directory
        self.fd = None
        self.no_sync = no_sync
        self.end = 0  # append offset
        self.dropped = 0  # entries dropped in memory but still in the file
        self.prev_index = 0  # the entry preceding entries[0]
        self.prev_term = 0
        self.entries = []  # in-memory window: prev_index+1 ...
        self.offs = []  # file offset of each entry's record

    @classmethod
    def open(cls, directory: str, no_sync: bool) -> "LogStore":
        try:
            os.remove(log_path(directory) + ".tmp")
        except FileNotFoundError:
            pass
        l = cls(directory, no_sync)
        try:
            with open(log_path(directory), "rb") as f:
                data = f.read()
        except FileNotFoundError:
            l.rewrite()
            return l
        if len(data) < LOG_HEADER_SIZE or zlib.crc32(data[:16]) != _U32.unpack_from(data, 16)[0]:
            raise ValueError("raft log: corrupt header")
        l.prev_index, l.prev_term = _HDR.unpack_from(data, 0)
        pos = LOG_HEADER_SIZE
        nxt = l.prev_index + 1
        while len(data) - pos >= 8:
            n, crc = _REC.unpack_from(data, pos)
            kind = KIND_CONFIG if n & _CONFIG_FLAG else KIND_NORMAL
            n &= ~_CONFIG_FLAG
            if n < 16 or len(data) - pos - 8 < n:
                break
            payload = data[pos + 8:pos + 8 + n]
            if zlib.crc32(payload) != crc:
                break
            term, index = _TI.unpack_from(payload, 0)
            if index != nxt:
                break
            body = bytes(payload[16:])
            l.entries.append(Entry(term, index, body if body else None, kind))
            l.offs.append(pos)
            pos += 8 + n
            nxt += 1
        fd = os.open(log_path(directory), os.O_RDWR)
        try:
            os.ftruncate(fd, pos)  # drop a torn tail
        except OSError:
            os.close(fd)
            raise
        l.fd, l.end = fd, pos
        return l

    def close(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def sync(self) -> None:
        if not self.no_sync:
            os.fsync(self.fd)

    def last_index(self) -> int:
        return self.prev_index + len(self.entries)

    def last_term(self) -> int:
        return self.entries[-1].term if self.entries else self.prev_term

    def term(self, index: int):
        """The term of the entry at index, or None if it is unknown."""
        if index == self.prev_index:
            return self.prev_term
        if self.prev_index < index <= self.last_index():
            return self.entries[index - self.prev_index - 1].term
        return None

    def slice(self, frm: int, max_n: int, max_bytes: int):
        """Entries from index `frm` on, up to max_n entries / max_bytes (at least one)."""
        if frm <= self.prev_index or frm > self.last_index():
            return []
        i = frm - self.prev_index - 1
        size = 0
        j = i
        n = len(self.entries)
        while j < n and j - i < max_n:
            e = self.entries[j]
            size += (len(e.data) if e.data else 0) + 24
            j += 1
            if size >= max_bytes:
                break
        return self.entries[i:j]

    def last_index_of_term(self, term: int) -> int:
        for e in reversed(self.entries):
            if e.term == term:
                return e.index
            if e.term < term:
                break
        return 0

    def first_index_of_term(self, term: int) -> int:
        first = 0
        for e in reversed(self.entries):
            if e.term != term:
                if first != 0:
                    break
                continue
            first = e.index
        return first

    def append(self, entries) -> None:
        """Durably add entries; their indices must continue the log."""
        if not entries:
            return
        if entries[0].index != self.last_index() + 1:
            raise ValueError("raft log: append at %d, expected %d" % (entries[0].index, self.last_index() + 1))
        buf = bytearray()
        offs = []
        for e in entries:
            offs.append(self.end + len(buf))
            buf += encode_record(e)
        pwrite_all(self.fd, buf, self.end)
        self.sync()
        self.end += len(buf)
        self.entries.extend(entries)
        self.offs.extend(offs)

    def truncate_from(self, index: int) -> None:
        """Remove entry `index` and everything after it."""
        if index <= self.prev_index or index > self.last_index():
            return
        pos = index - self.prev_index - 1
        off = self.offs[pos]
        os.ftruncate(self.fd, off)
        self.sync()
        del self.entries[pos:]
        del self.offs[pos:]
        self.end = off

    def compact_to(self, index: int) -> None:
        """Drop entries <= index from the in-memory window."""
        if index <= self.prev_index or index > self.last_index():
            return
        cut = index - self.prev_index
        term = self.entries[cut - 1].term
        self.entries = self.entries[cut:]
        self.offs = self.offs[cut:]
        self.prev_index, self.prev_term = index, term
        self.dropped += cut
        if self.dropped >= 1024 and self.dropped >= len(self.entries):
            self.rewrite()

    def reset(self, prev_index: int, prev_term: int) -> None:
        """Discard the whole log; the next entry will have index prev_index+1."""
        self.entries, self.offs = [], []
        self.prev_index, self.prev_term = prev_index, prev_term
        self.rewrite()

    def rewrite(self) -> None:
        """Atomically replace the file with header + current in-memory entries."""
        buf = bytearray(encode_header(self.prev_index, self.prev_term))
        offs = []
        for e in self.entries:
            offs.append(len(buf))
            buf += encode_record(e)
        tmp = log_path(self.dir) + ".tmp"
        fd = os.open(tmp, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
        try:
            write_all(fd, buf)
            os.fsync(fd)
            os.rename(tmp, log_path(self.dir))
            sync_dir(self.dir)
        except BaseException:
            os.close(fd)
            raise
        if self.fd is not None:
            os.close(self.fd)
        self.fd, self.offs, self.end, self.dropped = fd, offs, len(buf), 0
