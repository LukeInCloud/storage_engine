"""Immutable sorted files.

File layout:

    [data block | crc32]...  [bloom]  [index]  [footer]

A data block is a run of encoded entries (~BLOCK_SIZE bytes) in key order.
The index holds, per block, its first key, offset and length (incl. crc).
The index section also stores the largest key of the file (after the block
entries), so a table knows its key range [smallest, largest].
Footer (40 B): index_off, index_len, bloom_off, bloom_len, magic (u64 LE).
"""

import bisect
import os
import struct
import threading
import zlib

from ..util import VarintError, append_uvarint, pread_exact, read_uvarint, sync_dir, write_all
from .bloom import bloom_may_contain, build_bloom, hash_key
from .codec import Entry, append_entry, decode_entry
from .errors import CorruptError

BLOCK_SIZE = 4096
FOOTER = struct.Struct("<5Q")
FOOTER_SIZE = FOOTER.size  # 40
SST_MAGIC = 0x6B76737374303032
_CRC = struct.Struct("<I")


def sst_path(directory: str, fid: int) -> str:
    return os.path.join(directory, "%06d.sst" % fid)


class Block:
    """A decoded data block: parallel lists, shared (cache) and never modified."""

    __slots__ = ("keys", "entries")

    def __init__(self, entries):
        self.entries = entries
        self.keys = [e.key for e in entries]


# ---- writer ----


class SSTWriter:
    def __init__(self, path: str):
        self.f = open(path, "wb", buffering=64 << 10)
        self.off = 0
        self.block = bytearray()
        self.block_first = b""
        self.index = []  # (first_key, off, len)
        self.hashes = []
        self.last_key = None
        self.count = 0

    def _write(self, p) -> None:
        self.f.write(p)
        self.off += len(p)

    def add(self, e: Entry) -> None:
        if self.count > 0 and e.key <= self.last_key:
            raise ValueError("sstable: keys not strictly ascending")
        if not self.block:
            self.block_first = e.key
        append_entry(self.block, e)
        self.hashes.append(hash_key(e.key))
        self.last_key = e.key
        self.count += 1
        if len(self.block) >= BLOCK_SIZE:
            self._flush_block()

    def _flush_block(self) -> None:
        if not self.block:
            return
        start = self.off
        self._write(self.block)
        self._write(_CRC.pack(zlib.crc32(self.block)))
        self.index.append((self.block_first, start, self.off - start))
        self.block = bytearray()

    def finish(self) -> None:
        self._flush_block()
        bloom = build_bloom(self.hashes)
        bloom_off = self.off
        self._write(bloom)
        idx = bytearray()
        append_uvarint(idx, len(self.index))
        for first_key, off, ln in self.index:
            append_uvarint(idx, len(first_key))
            idx += first_key
            append_uvarint(idx, off)
            append_uvarint(idx, ln)
        last = self.last_key or b""
        append_uvarint(idx, len(last))
        idx += last
        index_off = self.off
        self._write(idx)
        self._write(FOOTER.pack(index_off, len(idx), bloom_off, len(bloom), SST_MAGIC))
        self.f.flush()
        os.fsync(self.f.fileno())
        self.f.close()

    def abort(self) -> None:
        try:
            self.f.close()
        except OSError:
            pass

    def size(self) -> int:
        """Number of bytes this table will roughly occupy so far."""
        return self.off + len(self.block)


def finish_table(w: SSTWriter, path: str, fid: int, cache) -> "SSTable":
    """Seal w and open the resulting table."""
    try:
        w.finish()
    except BaseException:
        w.abort()
        _remove(path)
        raise
    return open_sstable(path, fid, cache)


def write_table(directory: str, fid: int, entries, cache) -> "SSTable":
    """Drain entries into a new durable SSTable and open it."""
    path = sst_path(directory, fid)
    w = SSTWriter(path)
    try:
        for e in entries:
            w.add(e)
    except BaseException:
        w.abort()
        _remove(path)
        raise
    t = finish_table(w, path, fid, cache)
    try:
        sync_dir(directory)
    except OSError:
        t.unref()
        raise
    return t


def _remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


# ---- reader ----


class SSTable:
    """An immutable file. It is reference counted: the DB owns one reference
    and every reader/compaction takes another, so a file replaced by
    compaction is closed and deleted only after the last reader is done."""

    def __init__(self, fd, path, fid, size, cache):
        self.fd = fd
        self.path = path
        self.id = fid
        self.size = size
        self.cache = cache
        self.index = []  # (first_key, off, len)
        self.first_keys = []
        self.smallest = None  # key range; None if the table is empty
        self.largest = b""
        self.bloom = b""
        self.obsolete = False
        self._refs = 1
        self._lock = threading.Lock()

    def ref(self) -> None:
        with self._lock:
            self._refs += 1

    def unref(self) -> None:
        with self._lock:
            self._refs -= 1
            last = self._refs == 0
        if last:
            os.close(self.fd)
            if self.obsolete:
                _remove(self.path)

    def covers(self, key: bytes) -> bool:
        return self.smallest is not None and self.smallest <= key <= self.largest

    def read_block(self, i: int, fill: bool = True) -> Block:
        """Return the decoded block; the result is shared (cache) and must not
        be modified. fill=False (compaction scans) avoids evicting hot blocks."""
        key = (self.id, i)
        if self.cache is not None:
            blk = self.cache.get(key)
            if blk is not None:
                return blk
        _first, off, ln = self.index[i]
        buf = pread_exact(self.fd, ln, off)
        body, crc = buf[:-4], _CRC.unpack(buf[-4:])[0]
        if zlib.crc32(body) != crc:
            raise CorruptError("sstable %06d block %d: engine: corrupt data" % (self.id, i))
        ents = []
        pos = 0
        n = len(body)
        while pos < n:
            e, pos = decode_entry(body, pos)
            ents.append(e)
        blk = Block(ents)
        if fill and self.cache is not None:
            self.cache.put(key, blk, ln)
        return blk

    def block_for(self, key: bytes) -> int:
        """Index of the only block that may contain key, or -1."""
        return bisect.bisect_right(self.first_keys, key) - 1

    def get(self, key: bytes):
        if not bloom_may_contain(self.bloom, hash_key(key)):
            return None
        b = self.block_for(key)
        if b < 0:
            return None
        blk = self.read_block(b, True)
        j = bisect.bisect_left(blk.keys, key)
        if j < len(blk.keys) and blk.keys[j] == key:
            return blk.entries[j]
        return None

    def iter(self, start=None, fill: bool = True):
        """Entries with key >= start in order, reading blocks lazily."""
        bi = 0
        if start is not None:
            b = self.block_for(start)
            if b > 0:
                bi = b
        for i in range(bi, len(self.index)):
            blk = self.read_block(i, fill)
            pos = 0
            if start is not None and i == bi:
                pos = bisect.bisect_left(blk.keys, start)
            ents = blk.entries
            for j in range(pos, len(ents)):
                yield ents[j]


def open_sstable(path: str, fid: int, cache) -> SSTable:
    fd = os.open(path, os.O_RDONLY)
    try:
        t = _load_sstable(fd, path, fid, cache)
    except BaseException as e:
        os.close(fd)
        if isinstance(e, (CorruptError, EOFError)):
            raise CorruptError("%s: engine: corrupt data" % path) from None
        raise
    return t


def _load_sstable(fd: int, path: str, fid: int, cache) -> SSTable:
    size = os.fstat(fd).st_size
    if size < FOOTER_SIZE:
        raise CorruptError()
    index_off, index_len, bloom_off, bloom_len, magic = FOOTER.unpack(pread_exact(fd, FOOTER_SIZE, size - FOOTER_SIZE))
    if magic != SST_MAGIC:
        raise CorruptError()
    limit = size - FOOTER_SIZE
    if index_off + index_len > limit or bloom_off + bloom_len > limit:
        raise CorruptError()
    t = SSTable(fd, path, fid, size, cache)
    t.bloom = pread_exact(fd, bloom_len, bloom_off)
    idx = pread_exact(fd, index_len, index_off)
    try:
        cnt, pos = read_uvarint(idx, 0)
        for _ in range(cnt):
            kl, pos = read_uvarint(idx, pos)
            if kl > len(idx) - pos:
                raise CorruptError()
            first = idx[pos:pos + kl]
            pos += kl
            off, pos = read_uvarint(idx, pos)
            ln, pos = read_uvarint(idx, pos)
            if ln < 4 or off + ln > index_off:
                raise CorruptError()
            t.index.append((first, off, ln))
            t.first_keys.append(first)
        kl, pos = read_uvarint(idx, pos)
        if kl > len(idx) - pos:
            raise CorruptError()
        t.largest = idx[pos:pos + kl]
    except VarintError:
        raise CorruptError() from None
    if t.index:
        t.smallest = t.index[0][0]
    return t
