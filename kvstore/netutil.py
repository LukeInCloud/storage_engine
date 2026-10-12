"""Length-prefixed framing and a small threaded frame server, shared by the
client protocol (kvnet) and the Raft peer RPC."""

import logging
import socket
import struct
import threading

log = logging.getLogger("kvstore.net")

_LEN = struct.Struct("<I")


class FrameTooLargeError(Exception):
    def __init__(self):
        super().__init__("frame too large")


def parse_addr(addr: str):
    """"host:port" or "[v6]:port" -> (host, port); an empty host means all interfaces."""
    host, _, port = addr.rpartition(":")
    return host.strip("[]"), int(port)


def format_addr(sockname) -> str:
    host, port = sockname[0], sockname[1]
    return "[%s]:%d" % (host, port) if ":" in host else "%s:%d" % (host, port)


def set_nodelay(sock: socket.socket) -> None:
    """Best effort: on some platforms this fails for a connection that was
    already reset, which must not take down the accept loop."""
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError:
        pass


def encode_frame(payload: bytes) -> bytes:
    """Frame: len(u32 LE) | payload."""
    return _LEN.pack(len(payload)) + payload


class FrameReader:
    """Buffered frame reader over a socket."""

    def __init__(self, sock: socket.socket, max_frame: int, bufsize: int = 64 << 10):
        self.sock = sock
        self.max_frame = max_frame
        self.bufsize = bufsize
        self.buf = bytearray()

    def buffered(self) -> int:
        return len(self.buf)

    def _fill(self, n: int) -> None:
        while len(self.buf) < n:
            chunk = self.sock.recv(max(self.bufsize, min(n - len(self.buf), 1 << 20)))
            if not chunk:
                raise EOFError("connection closed")
            self.buf += chunk

    def read_frame(self) -> bytes:
        self._fill(4)
        (n,) = _LEN.unpack_from(self.buf, 0)
        if n > self.max_frame:
            raise FrameTooLargeError()
        self._fill(4 + n)
        payload = bytes(self.buf[4:4 + n])
        del self.buf[:4 + n]
        return payload


class FrameServer:
    """Accepts TCP connections and answers every request frame with
    handle(request) -> response bytes, one thread per connection. Requests on
    one connection are answered in order."""

    def __init__(self, addr: str, handle, max_frame: int, name: str = "frames"):
        host, port = parse_addr(addr)
        self.handle = handle
        self.max_frame = max_frame
        self.name = name
        self.ln = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.ln.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.ln.bind((host, port))
            self.ln.listen(128)
        except OSError:
            self.ln.close()
            raise
        self.ln.settimeout(0.2)  # lets the accept loop notice close()
        self._mu = threading.Lock()
        self._conns = set()
        self._handlers = set()
        self._closed = False
        self._accept = threading.Thread(target=self._accept_loop, name=name + "-accept", daemon=True)
        self._accept.start()

    def addr(self) -> str:
        return format_addr(self.ln.getsockname())

    def close(self) -> None:
        """Stop accepting, drop all connections and wait for handlers to end."""
        with self._mu:
            self._closed = True
            conns = list(self._conns)
        for c in conns:
            try:
                c.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self._accept.join()
        self.ln.close()
        with self._mu:
            handlers = list(self._handlers)
        for t in handlers:
            if t is not threading.current_thread():
                t.join()

    def _accept_loop(self) -> None:
        while True:
            with self._mu:
                if self._closed:
                    return
            try:
                c, _ = self.ln.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            c.settimeout(None)
            set_nodelay(c)
            with self._mu:
                if self._closed:
                    c.close()
                    return
                self._conns.add(c)
                t = threading.Thread(target=self._serve_conn, args=(c,), name=self.name + "-conn", daemon=True)
                self._handlers.add(t)
            t.start()

    def _serve_conn(self, c: socket.socket) -> None:
        try:
            r = FrameReader(c, self.max_frame)
            out = bytearray()
            while True:
                try:
                    req = r.read_frame()
                except (EOFError, OSError):
                    return
                except Exception as e:
                    log.warning("%s: connection dropped: %s", self.name, e)
                    return
                out += encode_frame(self.handle(req))
                # Flush only when no further pipelined request is already waiting.
                if r.buffered() == 0:
                    try:
                        c.sendall(out)
                    except OSError:
                        return
                    out.clear()
        finally:
            try:
                c.close()
            except OSError:
                pass
            with self._mu:
                self._conns.discard(c)
                self._handlers.discard(threading.current_thread())
