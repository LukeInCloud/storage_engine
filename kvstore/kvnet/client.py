import socket
import threading
import time

from ..engine.errors import NotFoundError
from ..engine.db import KV
from ..netutil import set_nodelay
from ..util import append_uvarint
from .protocol import (OP_BATCH_PUT, OP_DELETE, OP_PUT, OP_RANGE, OP_READ, ST_NOT_FOUND,
                       ST_NOT_LEADER, ST_OK, Dec, FrameReader, MalformedError, NotLeaderError,
                       ServerError, append_bytes, encode_frame)
from .server import parse_addr


class Client:
    """Safe for concurrent use; calls on one Client are serialised over a
    single connection (use several Clients for parallelism).

    A Client made by dial() talks to one server: a broken connection is
    re-dialled on the next call and the failed call is not retried. A Client
    made by dial_cluster() knows all nodes of a replicated cluster: it follows
    not-leader redirects, rotates to the next node when one is unreachable and
    retries with backoff for up to retry_for seconds. Retrying can apply a
    write twice if the first attempt's outcome was unknown; all operations here
    are idempotent (last write wins), so this is safe unless clients race on a
    key."""

    def __init__(self, addrs, timeout: float, retry_for: float = 0.0):
        self.timeout = timeout
        self.retry_for = retry_for
        self.mu = threading.Lock()
        self.addrs = list(addrs)
        self.cur = 0
        self.conn = None
        self.reader = None

    # ---- connection management (mu held) ----

    def _connect(self) -> None:
        host, port = parse_addr(self.addrs[self.cur])
        conn = socket.create_connection((host or "127.0.0.1", port), timeout=self.timeout)
        set_nodelay(conn)
        self.conn = conn
        self.reader = FrameReader(conn)

    def close(self) -> None:
        with self.mu:
            self._drop_conn()

    def _drop_conn(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except OSError:
                pass
            self.conn = None
            self.reader = None

    def _rotate(self) -> None:
        """Move on to the next known node."""
        self._drop_conn()
        self.cur = (self.cur + 1) % len(self.addrs)

    def _follow(self, addr: str) -> None:
        """Switch to the leader a node pointed us at (or rotate if unknown)."""
        if addr == "":
            self._rotate()
            return
        if addr in self.addrs:
            i = self.addrs.index(addr)
            if i != self.cur:
                self._drop_conn()
                self.cur = i
            return
        self._drop_conn()
        self.addrs.append(addr)
        self.cur = len(self.addrs) - 1

    def _attempt(self, req: bytes):
        if self.conn is None:
            self._connect()
        try:
            self.conn.settimeout(self.timeout)
            self.conn.sendall(encode_frame(req))
            resp = self.reader.read_frame()
        except (OSError, EOFError):
            self._drop_conn()
            raise
        if len(resp) == 0:
            self._drop_conn()
            raise MalformedError()
        return resp[0], resp[1:]

    def _do(self, req: bytes):
        """Send one request; returns (status, body). Raises on transport errors."""
        with self.mu:
            deadline = time.monotonic() + self.retry_for
            backoff = 0.02
            while True:
                err = None
                st, body = 0, b""
                try:
                    st, body = self._attempt(req)
                except (OSError, EOFError, MalformedError) as e:
                    err = e
                    self._rotate()
                    retry = True
                else:
                    retry = False
                    if st == ST_NOT_LEADER:
                        d = Dec(body)
                        self._follow(d.bytes().decode())
                        retry = True
                if not retry or self.retry_for == 0 or not time.monotonic() + backoff < deadline:
                    if err is not None:
                        raise err
                    return st, body
                time.sleep(backoff)
                backoff = min(backoff * 2, 0.5)

    @staticmethod
    def _check(status: int, body: bytes) -> None:
        """Convert a non-OK status into an exception."""
        if status == ST_OK:
            return
        if status == ST_NOT_FOUND:
            raise NotFoundError()
        d = Dec(body)
        if status == ST_NOT_LEADER:
            raise NotLeaderError(d.bytes().decode())
        msg = d.bytes()
        if d.err:
            raise MalformedError()
        raise ServerError(msg.decode(errors="replace"))

    def _simple(self, req) -> None:
        st, body = self._do(bytes(req))
        self._check(st, body)

    # ---- API ----

    def put(self, key: bytes, value: bytes) -> None:
        self._simple(append_bytes(append_bytes(bytearray([OP_PUT]), key), value))

    def delete(self, key: bytes) -> None:
        self._simple(append_bytes(bytearray([OP_DELETE]), key))

    def read(self, key: bytes) -> bytes:
        st, body = self._do(bytes(append_bytes(bytearray([OP_READ]), key)))
        self._check(st, body)
        d = Dec(body)
        v = d.bytes()
        if d.err:
            raise d.err
        return bytes(v)

    def batch_put(self, keys, values) -> None:
        if len(keys) != len(values):
            raise ValueError("kvnet: %d keys but %d values" % (len(keys), len(values)))
        req = bytearray([OP_BATCH_PUT])
        append_uvarint(req, len(keys))
        for k, v in zip(keys, values):
            append_bytes(req, k)
            append_bytes(req, v)
        self._simple(req)

    def read_key_range(self, start=None, end=None):
        """Pairs with start <= key <= end; None bounds are open."""
        flags = (1 if start is not None else 0) | (2 if end is not None else 0)
        req = bytearray([OP_RANGE, flags])
        if start is not None:
            append_bytes(req, start)
        if end is not None:
            append_bytes(req, end)
        st, body = self._do(bytes(req))
        self._check(st, body)
        d = Dec(body)
        n = d.uvarint()
        if d.err or n > len(body):
            raise MalformedError()
        out = []
        for _ in range(n):
            k, v = d.bytes(), d.bytes()
            if d.err:
                raise d.err
            out.append(KV(bytes(k), bytes(v)))
        return out

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def dial(addr: str, timeout: float = 10.0) -> Client:
    """Connect to a single server. timeout bounds every request (<=0 = 10 s)."""
    if timeout <= 0:
        timeout = 10.0
    c = Client([addr], timeout)
    with c.mu:
        c._connect()
    return c


def dial_cluster(addrs, timeout: float = 10.0, retry_for: float = 0.0) -> Client:
    """Create a leader-following client for a replicated cluster. Connections
    are made lazily, so some nodes may be down."""
    if not addrs:
        raise ValueError("kvnet: no addresses")
    if timeout <= 0:
        timeout = 10.0
    return Client(addrs, timeout, retry_for)
