from typing import NamedTuple

from ..util import VarintError, append_uvarint, read_uvarint
from .errors import CorruptError

KIND_DELETE = 0
KIND_PUT = 1


class Entry(NamedTuple):
    """The unit stored in every layer (WAL, memtable, SSTable).

    A delete is stored as a tombstone (KIND_DELETE) that shadows older versions.
    """

    key: bytes
    value: bytes
    kind: int


def append_entry(dst: bytearray, e: Entry) -> None:
    """Encode: kind(1) | klen(uvarint) | vlen(uvarint) | key | value."""
    dst.append(e.kind)
    append_uvarint(dst, len(e.key))
    append_uvarint(dst, len(e.value))
    dst += e.key
    dst += e.value


def decode_entry(b, pos: int = 0):
    """Decode one entry at b[pos:]; returns (Entry, new_pos). Raises CorruptError."""
    n = len(b)
    if pos >= n or b[pos] > KIND_PUT:
        raise CorruptError()
    kind = b[pos]
    try:
        kl, p = read_uvarint(b, pos + 1)
        vl, p = read_uvarint(b, p)
    except VarintError:
        raise CorruptError() from None
    rest = n - p
    if kl > rest or vl > rest - kl:
        raise CorruptError()
    key = bytes(b[p:p + kl])
    p += kl
    val = bytes(b[p:p + vl])
    return Entry(key, val, kind), p + vl
