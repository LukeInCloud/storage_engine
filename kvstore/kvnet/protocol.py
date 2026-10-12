"""Network layer: a compact length-prefixed binary protocol over TCP.

Frame:    len(u32 LE) | payload                     (len <= MAX_FRAME)
Request:  op(1) | args
Response: status(1) | body

Strings/bytes are encoded as uvarint length + data. Requests on one
connection are answered in order, so clients may pipeline.
"""

from ..netutil import FrameReader as _FrameReader
from ..netutil import FrameTooLargeError, encode_frame  # noqa: F401  (re-exported)
from ..util import VarintError, append_uvarint, read_uvarint

OP_PUT = 1  # key, value
OP_READ = 2  # key
OP_RANGE = 3  # flags(bit0 start, bit1 end), [start], [end]  (inclusive)
OP_BATCH_PUT = 4  # n, n x (key, value)
OP_DELETE = 5  # key

ST_OK = 0
ST_NOT_FOUND = 1
ST_ERROR = 2  # body: message
ST_NOT_LEADER = 3  # body: leader address ("" if unknown)

# MAX_FRAME bounds a single request or response (guards against bad peers).
MAX_FRAME = 64 << 20


class NotLeaderError(Exception):
    """Returned by a replicated Store on a follower; leader is its client address."""

    def __init__(self, leader: str = ""):
        super().__init__("kvnet: not the leader (leader=%r)" % leader)
        self.leader = leader


class MalformedError(Exception):
    def __init__(self):
        super().__init__("kvnet: malformed message")


class ServerError(Exception):
    """An error reported by the server (status ST_ERROR)."""


class FrameReader(_FrameReader):
    def __init__(self, sock, bufsize: int = 64 << 10):
        super().__init__(sock, MAX_FRAME, bufsize)


def append_bytes(dst: bytearray, b) -> bytearray:
    append_uvarint(dst, len(b))
    dst += b
    return dst


class Dec:
    """Sticky-error decoder: after the first failure every call returns a zero
    value and `err` stays set."""

    def __init__(self, b: bytes):
        self.b = b
        self.pos = 0
        self.err = None

    def byte(self) -> int:
        if self.err is not None or self.pos >= len(self.b):
            self.err = MalformedError()
            return 0
        c = self.b[self.pos]
        self.pos += 1
        return c

    def uvarint(self) -> int:
        if self.err is not None:
            return 0
        try:
            v, self.pos = read_uvarint(self.b, self.pos)
        except VarintError:
            self.err = MalformedError()
            return 0
        return v

    def bytes(self) -> bytes:
        n = self.uvarint()
        if self.err is not None:
            return b""
        if n > len(self.b) - self.pos:
            self.err = MalformedError()
            return b""
        v = self.b[self.pos:self.pos + n]
        self.pos += n
        return v

    def remaining(self) -> int:
        return len(self.b) - self.pos
