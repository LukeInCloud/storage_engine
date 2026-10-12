import os
import struct
import threading
import zlib

from ..util import VarintError, append_uvarint, read_uvarint, sync_dir, write_all
from .codec import append_entry, decode_entry
from .errors import CorruptError

# CRC-32 (zlib, computed in C) protects every record.
WAL_HEADER = struct.Struct("<II")  # payload length | crc32 of payload


def wal_path(directory: str, num: int) -> str:
    return os.path.join(directory, "wal-%06d.log" % num)


class WAL:
    """Append-only log. One record == one batch, so a batch is atomic: after a
    crash it is either replayed completely or not at all."""

    def __init__(self, fd: int, num: int, sync: bool):
        self.fd = fd
        self.num = num
        self.sync_writes = sync
        self._lock = threading.Lock()
        self._closed = False

    def append(self, batches) -> None:
        """One write and one fsync for all batches (group commit); each record
        stays individually atomic."""
        buf = bytearray()
        for batch in batches:
            payload = bytearray()
            append_uvarint(payload, len(batch))
            for e in batch:
                append_entry(payload, e)
            buf += WAL_HEADER.pack(len(payload), zlib.crc32(payload))
            buf += payload
        with self._lock:
            if self._closed:
                raise OSError("wal closed")
            write_all(self.fd, buf)
            if self.sync_writes:
                os.fsync(self.fd)

    def sync(self) -> None:
        """fsync; a WAL closed meanwhile was flushed to an SSTable, which is already durable."""
        with self._lock:
            if not self._closed:
                os.fsync(self.fd)

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                os.close(self.fd)


def create_wal(directory: str, num: int, sync: bool) -> WAL:
    fd = os.open(wal_path(directory, num), os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
    try:
        sync_dir(directory)
    except OSError:
        os.close(fd)
        raise
    return WAL(fd, num, sync)


def decode_batch(p: bytes):
    try:
        cnt, pos = read_uvarint(p, 0)
    except VarintError:
        raise CorruptError() from None
    out = []
    for _ in range(cnt):
        e, pos = decode_entry(p, pos)
        out.append(e)
    return out


def replay_wal(path: str, apply) -> None:
    """Apply every intact record. Stops silently at the first torn or corrupt
    record: that is the tail of a write interrupted by a crash."""
    with open(path, "rb") as f:
        data = f.read()
    pos = 0
    hs = WAL_HEADER.size
    while len(data) - pos >= hs:
        n, crc = WAL_HEADER.unpack_from(data, pos)
        if len(data) - pos - hs < n:
            break
        payload = data[pos + hs:pos + hs + n]
        if zlib.crc32(payload) != crc:
            break
        try:
            batch = decode_batch(payload)
        except CorruptError:
            break
        apply(batch)
        pos += hs + n
