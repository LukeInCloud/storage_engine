from ..engine.errors import NotFoundError
from ..netutil import FrameServer, parse_addr  # noqa: F401  (parse_addr re-exported)
from ..util import append_uvarint
from .protocol import (MAX_FRAME, OP_BATCH_PUT, OP_DELETE, OP_PUT, OP_RANGE, OP_READ, ST_ERROR,
                       ST_NOT_FOUND, ST_NOT_LEADER, ST_OK, Dec, MalformedError, NotLeaderError,
                       append_bytes)


def err_response(err: Exception) -> bytes:
    if isinstance(err, NotFoundError):
        return bytes([ST_NOT_FOUND])
    if isinstance(err, NotLeaderError):
        return bytes(append_bytes(bytearray([ST_NOT_LEADER]), err.leader.encode()))
    return bytes(append_bytes(bytearray([ST_ERROR]), str(err).encode()))


OK_RESPONSE = bytes([ST_OK])


class Server(FrameServer):
    """Serves any store with put/read/read_key_range/batch_put/delete over TCP."""

    def __init__(self, store, addr: str):
        self.store = store
        super().__init__(addr, self.handle, MAX_FRAME, name="kvnet")

    def handle(self, req: bytes) -> bytes:
        d = Dec(req)
        op = d.byte()
        try:
            if op == OP_PUT:
                k, v = d.bytes(), d.bytes()
                if d.err:
                    return err_response(d.err)
                self.store.put(k, v)
                return OK_RESPONSE
            if op == OP_READ:
                k = d.bytes()
                if d.err:
                    return err_response(d.err)
                v = self.store.read(k)
                return bytes(append_bytes(bytearray([ST_OK]), v))
            if op == OP_DELETE:
                k = d.bytes()
                if d.err:
                    return err_response(d.err)
                self.store.delete(k)
                return OK_RESPONSE
            if op == OP_BATCH_PUT:
                n = d.uvarint()
                if d.err or n > d.remaining():  # each pair takes >= 2 bytes
                    return err_response(MalformedError())
                keys, vals = [], []
                for _ in range(n):
                    keys.append(d.bytes())
                    vals.append(d.bytes())
                if d.err:
                    return err_response(d.err)
                self.store.batch_put(keys, vals)
                return OK_RESPONSE
            if op == OP_RANGE:
                flags = d.byte()
                start = end = None
                if flags & 1:
                    start = d.bytes()
                if flags & 2:
                    end = d.bytes()
                if d.err:
                    return err_response(d.err)
                kvs = self.store.read_key_range(start, end)
                out = bytearray([ST_OK])
                append_uvarint(out, len(kvs))
                for kv in kvs:
                    append_bytes(out, kv[0])
                    append_bytes(out, kv[1])
                    if len(out) > MAX_FRAME:
                        return err_response(Exception("kvnet: range result too large; narrow the range"))
                return bytes(out)
            return err_response(Exception("kvnet: unknown operation"))
        except Exception as e:
            return err_response(e)


def listen(addr: str, store) -> Server:
    """Start serving store on addr (e.g. "127.0.0.1:7000", or "127.0.0.1:0" for any port)."""
    return Server(store, addr)
