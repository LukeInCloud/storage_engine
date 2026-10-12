"""Peer RPC over TCP: a compact binary encoding in length-prefixed frames,
one thread per server connection, a small pool of connections per peer.

Request frame:  method(1) | body
Response frame: status(1) | body        (status 1: body is an error message)
"""

import socket
import threading

from ..netutil import FrameReader, FrameServer, encode_frame, parse_addr, set_nodelay
from ..util import VarintError, append_uvarint, read_uvarint
from .types import (AppendReq, AppendResp, Entry, KV, RaftError, RaftTimeoutError, SnapReq,
                    SnapResp, Transport, VoteReq, VoteResp)

M_REQUEST_VOTE = 1
M_APPEND_ENTRIES = 2
M_INSTALL_SNAPSHOT = 3

MAX_RPC_FRAME = 256 << 20
MAX_IDLE_PER_PEER = 4


class RemoteError(RaftError):
    """The peer's handler failed."""


# ---- encoding ----


class _W:
    def __init__(self):
        self.b = bytearray()

    def u(self, v: int):
        append_uvarint(self.b, v)

    def flag(self, v: bool):
        self.b.append(1 if v else 0)

    def raw(self, v: bytes):
        append_uvarint(self.b, len(v))
        self.b += v

    def s(self, v: str):
        self.raw(v.encode())


class _R:
    def __init__(self, b: bytes):
        self.b, self.pos = b, 0

    def u(self) -> int:
        try:
            v, self.pos = read_uvarint(self.b, self.pos)
        except VarintError as e:
            raise RemoteError("raft rpc: malformed message: %s" % e) from None
        return v

    def flag(self) -> bool:
        if self.pos >= len(self.b):
            raise RemoteError("raft rpc: malformed message")
        v = self.b[self.pos]
        self.pos += 1
        return v != 0

    def raw(self) -> bytes:
        n = self.u()
        if n > len(self.b) - self.pos:
            raise RemoteError("raft rpc: malformed message")
        v = self.b[self.pos:self.pos + n]
        self.pos += n
        return bytes(v)

    def s(self) -> str:
        return self.raw().decode()


def enc_vote_req(r: VoteReq) -> bytes:
    w = _W()
    w.u(r.term); w.s(r.candidate); w.u(r.last_log_index); w.u(r.last_log_term); w.flag(r.pre_vote)
    return bytes(w.b)


def dec_vote_req(b: bytes) -> VoteReq:
    r = _R(b)
    return VoteReq(r.u(), r.s(), r.u(), r.u(), r.flag())


def enc_vote_resp(x: VoteResp) -> bytes:
    w = _W()
    w.u(x.term); w.flag(x.granted)
    return bytes(w.b)


def dec_vote_resp(b: bytes) -> VoteResp:
    r = _R(b)
    return VoteResp(r.u(), r.flag())


def enc_append_req(x: AppendReq) -> bytes:
    w = _W()
    w.u(x.term); w.s(x.leader); w.u(x.prev_index); w.u(x.prev_term); w.u(x.commit)
    w.u(len(x.entries))
    for e in x.entries:
        w.u(e.term); w.u(e.index)
        w.flag(e.data is not None)
        if e.data is not None:
            w.raw(e.data)
    return bytes(w.b)


def dec_append_req(b: bytes) -> AppendReq:
    r = _R(b)
    req = AppendReq(r.u(), r.s(), r.u(), r.u(), [], r.u())
    n = r.u()
    if n > len(b):
        raise RemoteError("raft rpc: malformed message")
    for _ in range(n):
        term, index = r.u(), r.u()
        req.entries.append(Entry(term, index, r.raw() if r.flag() else None))
    return req


def enc_append_resp(x: AppendResp) -> bytes:
    w = _W()
    w.u(x.term); w.flag(x.success); w.u(x.conflict_index); w.u(x.conflict_term)
    return bytes(w.b)


def dec_append_resp(b: bytes) -> AppendResp:
    r = _R(b)
    return AppendResp(r.u(), r.flag(), r.u(), r.u())


def enc_snap_req(x: SnapReq) -> bytes:
    w = _W()
    w.u(x.term); w.s(x.leader); w.u(x.session); w.u(x.seq); w.u(x.snap_index); w.u(x.snap_term)
    w.flag(x.done)
    w.u(len(x.items))
    for kv in x.items:
        w.raw(kv.key); w.raw(kv.value)
    return bytes(w.b)


def dec_snap_req(b: bytes) -> SnapReq:
    r = _R(b)
    req = SnapReq(r.u(), r.s(), r.u(), r.u(), r.u(), r.u(), [], r.flag())
    n = r.u()
    if n > len(b):
        raise RemoteError("raft rpc: malformed message")
    for _ in range(n):
        req.items.append(KV(r.raw(), r.raw()))
    return req


def enc_snap_resp(x: SnapResp) -> bytes:
    w = _W()
    w.u(x.term); w.flag(x.ok); w.flag(x.skip)
    return bytes(w.b)


def dec_snap_resp(b: bytes) -> SnapResp:
    r = _R(b)
    return SnapResp(r.u(), r.flag(), r.flag())


# ---- client side ----


class _Conn:
    __slots__ = ("sock", "reader")

    def __init__(self, sock):
        self.sock = sock
        self.reader = FrameReader(sock, MAX_RPC_FRAME)


class TCPTransport(Transport):
    """A Transport over TCP with a pool of cached connections per peer.

    addrs maps node IDs to Raft RPC addresses. timeout (seconds) bounds each
    call; snapshot chunks get at least 10 s."""

    def __init__(self, addrs: dict, timeout: float = 1.0):
        self.addrs = dict(addrs)
        self.timeout = timeout if timeout > 0 else 1.0
        self._mu = threading.Lock()
        self._idle = {}  # peer -> [_Conn]
        self._closed = False

    def _take(self, to: str) -> _Conn:
        with self._mu:
            idle = self._idle.get(to)
            if idle:
                return idle.pop()
            addr = self.addrs.get(to)
        if addr is None:
            raise RaftError("raft: unknown peer " + to)
        host, port = parse_addr(addr)
        sock = socket.create_connection((host or "127.0.0.1", port), timeout=self.timeout)
        set_nodelay(sock)
        return _Conn(sock)

    def _give_back(self, to: str, c: _Conn) -> None:
        with self._mu:
            idle = self._idle.setdefault(to, [])
            if not self._closed and len(idle) < MAX_IDLE_PER_PEER:
                idle.append(c)
                return
        c.sock.close()

    def _call(self, to: str, method: int, timeout: float, body: bytes) -> bytes:
        c = self._take(to)
        try:
            c.sock.settimeout(timeout)
            c.sock.sendall(encode_frame(bytes([method]) + body))
            resp = c.reader.read_frame()
        except socket.timeout:
            c.sock.close()
            raise RaftTimeoutError() from None
        except BaseException:
            c.sock.close()
            raise
        self._give_back(to, c)
        if not resp:
            raise RemoteError("raft rpc: empty response")
        if resp[0] != 0:
            raise RemoteError(resp[1:].decode(errors="replace"))
        return resp[1:]

    def request_vote(self, to: str, req: VoteReq) -> VoteResp:
        return dec_vote_resp(self._call(to, M_REQUEST_VOTE, self.timeout, enc_vote_req(req)))

    def append_entries(self, to: str, req: AppendReq) -> AppendResp:
        return dec_append_resp(self._call(to, M_APPEND_ENTRIES, 2 * self.timeout, enc_append_req(req)))

    def install_snapshot(self, to: str, req: SnapReq) -> SnapResp:
        timeout = max(self.timeout, 10.0)
        return dec_snap_resp(self._call(to, M_INSTALL_SNAPSHOT, timeout, enc_snap_req(req)))

    def close(self) -> None:
        with self._mu:
            self._closed = True
            conns = [c for idle in self._idle.values() for c in idle]
            self._idle.clear()
        for c in conns:
            c.sock.close()


# ---- server side ----


class RPCServer(FrameServer):
    """Serves a Node's handlers to its peers."""

    def __init__(self, addr: str, node):
        self.node = node
        super().__init__(addr, self._handle, MAX_RPC_FRAME, name="raft-rpc")

    def _handle(self, req: bytes) -> bytes:
        try:
            if not req:
                raise RemoteError("raft rpc: empty request")
            method, body = req[0], req[1:]
            if method == M_REQUEST_VOTE:
                out = enc_vote_resp(self.node.handle_request_vote(dec_vote_req(body)))
            elif method == M_APPEND_ENTRIES:
                out = enc_append_resp(self.node.handle_append_entries(dec_append_req(body)))
            elif method == M_INSTALL_SNAPSHOT:
                out = enc_snap_resp(self.node.handle_install_snapshot(dec_snap_req(body)))
            else:
                raise RemoteError("raft rpc: unknown method %d" % method)
            return b"\x00" + out
        except Exception as e:
            return b"\x01" + (str(e) or type(e).__name__).encode()


def serve_rpc(addr: str, node) -> RPCServer:
    """Accept peer connections on addr until close()."""
    return RPCServer(addr, node)
