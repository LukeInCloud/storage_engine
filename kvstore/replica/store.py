import threading

from ..engine import EmptyKeyError, NotFoundError, Op
from ..kvnet.protocol import NotLeaderError as ClientNotLeaderError
from ..raft import NotLeaderError, RaftTimeoutError, StoppedError
from .machine import RESERVED_PREFIX, encode_command, is_reserved


class Store:
    """The replicated key/value store: it implements the kvnet store
    interface. Writes go through Raft; reads use ReadIndex, so every
    successful call is linearizable. On a follower every call fails with
    kvnet.NotLeaderError carrying the leader's client address, which clients
    follow."""

    def __init__(self, node, sm, timeout: float = 5.0):
        self.node = node
        self.sm = sm
        self.timeout = timeout if timeout > 0 else 5.0
        self._mu = threading.Lock()
        self._client_addrs = {}  # node ID -> client-facing address

    def set_client_addrs(self, m: dict) -> None:
        """Tell the store where each node serves clients, so that not-leader
        replies can point at the leader."""
        with self._mu:
            self._client_addrs = dict(m)

    def _translate(self, err: Exception) -> Exception:
        """Convert Raft errors into what clients should do about them."""
        if isinstance(err, NotLeaderError):
            with self._mu:
                addr = self._client_addrs.get(err.leader, "")
            return ClientNotLeaderError(addr)
        if isinstance(err, (RaftTimeoutError, StoppedError)):
            # No quorum / shutting down: tell the client to look elsewhere and retry.
            return ClientNotLeaderError("")
        return err

    @staticmethod
    def _check_key(k: bytes) -> None:
        if len(k) == 0:
            raise EmptyKeyError()
        if is_reserved(k):
            raise ValueError("replica: keys starting with %r are reserved" % RESERVED_PREFIX)

    def _propose(self, ops) -> None:
        for o in ops:
            self._check_key(o.key)
        try:
            self.node.propose(encode_command(ops), self.timeout)
        except Exception as e:
            raise self._translate(e) from None

    def put(self, key: bytes, value: bytes) -> None:
        self._propose([Op(key, value)])

    def delete(self, key: bytes) -> None:
        self._propose([Op(key, b"", True)])

    def batch_put(self, keys, values) -> None:
        if len(keys) != len(values):
            raise ValueError("replica: %d keys but %d values" % (len(keys), len(values)))
        self._propose([Op(k, v) for k, v in zip(keys, values)])

    def _barrier(self) -> None:
        try:
            self.node.read_barrier(self.timeout)
        except Exception as e:
            raise self._translate(e) from None

    def read(self, key: bytes) -> bytes:
        self._barrier()
        return self.sm.get(key)

    def read_key_range(self, start=None, end=None):
        self._barrier()
        return self.sm.range(start, end)
