"""A small push-pull gossip protocol for cluster membership and liveness.

Every node keeps a table  id -> Member(raft_addr, client_addr, version)  and
every `interval` it

  1. bumps its own heartbeat (the version of its own entry is
     (generation, heartbeat); the generation is the start time, so a restarted
     node's entries supersede the ones circulating from its previous life),
  2. picks `fanout` random peers and exchanges full tables with each: it sends
     its table and merges the table that comes back; the peer merges ours.

Entries only ever move to higher versions, so tables converge. A node that
learns something new (a member it did not know, or a change of address) wakes
its gossip loop at once, which spreads the news with an extra round
(rumour mongering): with fanout f, news reaches N nodes in O(log N) rounds.

Joining needs one contact point: the address of any member. join() exchanges
tables with it, which hands the newcomer the whole table and puts the
newcomer's entry into that member's table, from where gossip carries it to
everybody. Until the first exchange succeeds, every round retries the seeds.

Liveness: an entry whose version has not advanced for `fail_after` seconds
(locally measured) is reported as not alive; silent_for() gives the exact
time. Gossip itself never removes a member: the cluster does that by
agreement (see replica.server) and then calls forget(), which drops the entry
and ignores it until the node shows up with a newer generation, i.e. after a
restart.

The wire format is   count | (id raft_addr client_addr generation heartbeat)*
with varints and length-prefixed strings; the transport is injected:
exchange(addr, payload, timeout) -> response payload.
"""

import logging
import random
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from ..util import VarintError, append_uvarint, read_uvarint

_log = logging.getLogger("gossip")

MAX_MEMBERS = 1 << 16


@dataclass
class Member:
    id: str
    raft_addr: str
    client_addr: str
    generation: int = 0
    heartbeat: int = 0
    alive: bool = True  # filled in by Gossip.members()

    def version(self):
        return (self.generation, self.heartbeat)


def encode_table(members) -> bytes:
    b = bytearray()
    append_uvarint(b, len(members))
    for m in members:
        for s in (m.id, m.raft_addr, m.client_addr):
            raw = s.encode()
            append_uvarint(b, len(raw))
            b += raw
        append_uvarint(b, m.generation)
        append_uvarint(b, m.heartbeat)
    return bytes(b)


def decode_table(data: bytes) -> List[Member]:
    try:
        n, pos = read_uvarint(data, 0)
        if n > MAX_MEMBERS:
            raise ValueError("gossip: table too large")
        out = []
        for _ in range(n):
            parts = []
            for _ in range(3):
                ln, pos = read_uvarint(data, pos)
                if ln > len(data) - pos:
                    raise ValueError("gossip: malformed message")
                parts.append(bytes(data[pos:pos + ln]).decode())
                pos += ln
            gen, pos = read_uvarint(data, pos)
            hb, pos = read_uvarint(data, pos)
            if not parts[0]:
                raise ValueError("gossip: member without id")
            out.append(Member(parts[0], parts[1], parts[2], gen, hb))
        return out
    except VarintError:
        raise ValueError("gossip: malformed message") from None


class Gossip:
    def __init__(self, me: Member, exchange: Callable[[str, bytes, float], bytes], seeds=(),
                 interval: float = 0.5, fanout: int = 2, fail_after: float = 0.0,
                 timeout: float = 1.0, on_change: Optional[Callable[[Member], None]] = None):
        """me is this node's own entry; seeds are addresses to contact while
        the table holds no other member. on_change(member) is called (without
        any lock held) when a member is learned or its address changed."""
        self.exchange = exchange
        self.seeds = list(seeds)
        self.interval = interval
        self.fanout = max(1, fanout)
        self.fail_after = fail_after if fail_after > 0 else max(10 * interval, 3.0)
        self.timeout = timeout
        self.on_change = on_change
        self._mu = threading.Lock()
        self._table: Dict[str, Member] = {}
        self._seen: Dict[str, float] = {}  # id -> local monotonic time of its last version advance
        self._gone: Dict[str, int] = {}  # forgotten id -> generation it had; older/equal is ignored
        self.me = me
        me.generation = me.generation or int(time.time() * 1000)
        self._table[me.id] = me
        self._seen[me.id] = time.monotonic()
        self._rnd = random.Random()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = None

    # ---- table ----

    def _merge(self, incoming: List[Member]) -> List[Member]:
        """Merge entries into the table; returns those that were new or changed."""
        changed = []
        now = time.monotonic()
        with self._mu:
            for m in incoming:
                cur = self._table.get(m.id)
                if m.id in self._gone:
                    if m.generation <= self._gone[m.id]:
                        continue
                    del self._gone[m.id]  # it restarted: a new incarnation
                if m.id == self.me.id:
                    # Someone has a newer version of us (an earlier incarnation
                    # with a later clock): outrank it.
                    if m.version() > self.me.version():
                        self.me.generation, self.me.heartbeat = m.generation + 1, 0
                    continue
                if cur is None:
                    self._table[m.id] = Member(m.id, m.raft_addr, m.client_addr, m.generation, m.heartbeat)
                    self._seen[m.id] = now
                    changed.append(self._table[m.id])
                elif m.version() > cur.version():
                    moved = (cur.raft_addr, cur.client_addr) != (m.raft_addr, m.client_addr)
                    cur.raft_addr, cur.client_addr = m.raft_addr, m.client_addr
                    cur.generation, cur.heartbeat = m.generation, m.heartbeat
                    self._seen[m.id] = now
                    if moved:
                        changed.append(cur)
        if changed:
            self._wake.set()  # spread the news without waiting for the next tick
        return changed

    def _notify(self, changed: List[Member]) -> None:
        if self.on_change is not None:
            for m in changed:
                try:
                    self.on_change(Member(m.id, m.raft_addr, m.client_addr, m.generation, m.heartbeat))
                except Exception:
                    _log.exception("gossip: on_change")

    def observe(self, nid: str, raft_addr: str, client_addr: str) -> None:
        """Learn a member from another source (the Raft configuration). Its
        entry starts at version (0, 0): any real gossip from the node wins."""
        if nid == self.me.id:
            return
        with self._mu:
            self._gone.pop(nid, None)  # the configuration says it is a member
            if nid in self._table:
                return
            self._table[nid] = Member(nid, raft_addr, client_addr)
            self._seen[nid] = time.monotonic()

    def forget(self, nid: str) -> None:
        """The node was removed from the cluster: drop it and ignore gossip
        about it until it comes back with a newer generation."""
        if nid == self.me.id:
            return
        with self._mu:
            cur = self._table.pop(nid, None)
            self._seen.pop(nid, None)
            self._gone[nid] = cur.generation if cur is not None else self._gone.get(nid, 0)

    def silent_for(self, nid: str) -> float:
        """Seconds since the entry of nid last advanced (0 for this node,
        inf for a node that is not in the table)."""
        with self._mu:
            if nid == self.me.id:
                return 0.0
            if nid not in self._seen:
                return float("inf")
            return time.monotonic() - self._seen[nid]

    def members(self) -> List[Member]:
        """A copy of the table, sorted by id."""
        now = time.monotonic()
        with self._mu:
            out = []
            for m in self._table.values():
                c = Member(m.id, m.raft_addr, m.client_addr, m.generation, m.heartbeat)
                c.alive = m.id == self.me.id or now - self._seen[m.id] < self.fail_after
                out.append(c)
        return sorted(out, key=lambda m: m.id)

    # ---- protocol ----

    def handle(self, body: bytes) -> bytes:
        """Answer a peer's exchange (called by the RPC server)."""
        changed = self._merge(decode_table(body))
        self._notify(changed)
        return self._snapshot()

    def _snapshot(self) -> bytes:
        with self._mu:
            return encode_table(list(self._table.values()))

    def _exchange_with(self, addr: str) -> bool:
        try:
            resp = self.exchange(addr, self._snapshot(), self.timeout)
            changed = self._merge(decode_table(resp))
        except Exception as e:
            _log.debug("gossip: exchange with %s: %s", addr, e)
            return False
        self._notify(changed)
        return True

    def join(self, seed_addr: str) -> bool:
        """Introduce this node to the cluster through one known member.
        Returns True if the member answered (the table now lists the cluster)."""
        return self._exchange_with(seed_addr)

    def _round(self) -> None:
        with self._mu:
            self.me.heartbeat += 1
            peers = [m.raft_addr for m in self._table.values()
                     if m.id != self.me.id and m.raft_addr and m.raft_addr != self.me.raft_addr]
        if not peers:
            # Not connected to anybody yet: keep trying the seeds.
            for a in self.seeds:
                self._exchange_with(a)
            return
        for a in self._rnd.sample(peers, min(self.fanout, len(peers))):
            self._exchange_with(a)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(self.interval)
            self._wake.clear()
            if self._stop.is_set():
                return
            try:
                self._round()
            except Exception:
                _log.exception("gossip: round")

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="gossip", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join()
