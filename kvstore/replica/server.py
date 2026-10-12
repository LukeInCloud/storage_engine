import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from .. import kvnet
from ..engine import Options
from ..gossip import Gossip, Member
from ..raft import LEADER, Config, NotLeaderError, Node, TCPTransport, serve_rpc
from .machine import KVMachine
from .store import Store

_log = logging.getLogger("replica")


@dataclass
class Peer:
    """One cluster member."""

    id: str
    raft_addr: str  # node-to-node RPC
    client_addr: str  # clients (kvnet)


def parse_peers(s: str) -> List[Peer]:
    """Parse "id=raftHost:port/clientHost:port,id2=..."."""
    out = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        pid, eq, addrs = part.partition("=")
        r, slash, c = addrs.partition("/")
        if not eq or not slash or not pid or not r or not c:
            raise ValueError("bad peer %r, want id=raftAddr/clientAddr" % part)
        out.append(Peer(pid, r, c))
    return out


@dataclass
class NodeConfig:
    id: str
    dir: str  # node data dir: holds sm/ (engine data) and raft/ (log, state)
    peers: List[Peer]  # whole cluster, including this node (only this node when joining)
    engine: Options = field(default_factory=Options)
    tune: Optional[Callable[[Config], None]] = None  # optional tweaks (timeouts, retention, ...)
    logf: Optional[Callable] = None

    # Joining a running cluster: join is the Raft RPC address of any one
    # member. The node introduces itself to it by gossip, copies its state,
    # and is then added to the Raft configuration by the leader.
    join: str = ""
    join_timeout: float = 10.0  # how long start_server waits for the first contact
    gossip_interval: float = 0.5
    # A member silent (no gossip heartbeat) for this long is a candidate for
    # removal: the leader drops it once a majority of voters agree it is
    # silent for them too. 0 disables automatic removal.
    dead_after: float = 60.0


class Server:
    """One running cluster member."""

    def __init__(self):
        self.machine = None
        self.node = None
        self.store = None
        self.rpc = None
        self.transport = None
        self.clients = None
        self.gossip = None
        self._stop = threading.Event()
        self._reconciler = None
        self.dead_after = 0.0

    def members(self):
        """What this node's gossip knows: a list of gossip.Member (with .alive)."""
        return self.gossip.members()

    def is_dead(self, nid: str) -> bool:
        """This node's own opinion: nid has been silent for dead_after."""
        return self.dead_after > 0 and self.gossip.silent_for(nid) >= self.dead_after

    def _remove_dead(self) -> None:
        """Leader: remove a member that a majority of voters, itself
        included, have not heard of for dead_after. Done one at a time; the
        configuration entry then makes every node drop it from its peers."""
        mem = self.node.membership()
        for nid in sorted(mem.members):
            if nid == self.node.cfg.id or not self.is_dead(nid):
                continue
            voters = [v for v in mem.voters if v != nid]
            need = len(mem.voters) // 2 + 1
            agree = 1 if self.node.cfg.id in mem.voters else 0
            for v in voters:
                if v == self.node.cfg.id or agree >= need:
                    continue
                try:
                    if self.transport.ask_dead(v, nid):
                        agree += 1
                except Exception:
                    pass  # unreachable voters do not confirm
            if agree < need:
                continue  # a minority of the cluster must never shrink it
            _log.warning("removing dead member %s (confirmed by %d of %d voters)", nid, agree,
                         len(mem.voters))
            try:
                self.node.remove_member(nid)
            except NotLeaderError:
                pass
            return

    def _reconcile_loop(self) -> None:
        """The leader turns members learned by gossip into Raft members and
        drops members that are confirmed dead."""
        last_check = 0.0
        while not self._stop.wait(0.1):
            if self.node.status().role != LEADER:
                continue
            if self.dead_after > 0 and time.monotonic() - last_check >= 1.0:
                last_check = time.monotonic()
                try:
                    self._remove_dead()
                except Exception as e:
                    _log.warning("remove dead members: %s", e)
            known = self.node.membership().members
            for m in self.gossip.members():
                if m.id in known or not m.alive or not m.raft_addr:
                    continue
                try:
                    self.node.add_member(m.id, join_info(m.raft_addr, m.client_addr))
                except NotLeaderError:
                    break
                except Exception as e:
                    _log.warning("add member %s: %s", m.id, e)
                    break  # one change at a time

    def client_addr(self) -> str:
        return self.clients.addr()

    def close(self) -> None:
        self._stop.set()
        if self._reconciler is not None:
            self._reconciler.join()
        if self.gossip is not None:
            self.gossip.close()
        if self.clients is not None:
            self.clients.close()
        if self.node is not None:
            self.node.stop()
        if self.rpc is not None:
            self.rpc.close()
        if self.transport is not None:
            self.transport.close()
        if self.machine is not None:
            self.machine.close()


def join_info(raft_addr: str, client_addr: str) -> str:
    """The per-node info stored in Raft configurations."""
    return raft_addr + "/" + client_addr


def split_info(info: str):
    r, _, c = info.partition("/")
    return r, c


def start_server(cfg: NodeConfig) -> Server:
    self_peer = None
    raft_addrs, infos = {}, {}
    for p in cfg.peers:
        raft_addrs[p.id] = p.raft_addr
        infos[p.id] = join_info(p.raft_addr, p.client_addr)
        if p.id == cfg.id:
            self_peer = p
    if self_peer is None:
        raise ValueError("replica: node %r is not in the peer list" % cfg.id)

    s = Server()
    s.dead_after = cfg.dead_after
    client_addrs = {}
    known = set()  # members seen in the latest configurations
    addrs_mu = threading.Lock()

    def on_config(mem) -> None:
        # Runs inside the Raft node: only touch things that never call back.
        with addrs_mu:
            for nid in [n for n in known if n not in mem.members]:
                known.discard(nid)  # removed from the cluster
                client_addrs.pop(nid, None)
                if s.gossip is not None:
                    s.gossip.forget(nid)
            known.update(mem.members)
            for nid, info in mem.members.items():
                r, c = split_info(info)
                if nid != cfg.id and r:
                    s.transport.set_addr(nid, r)
                    if s.gossip is not None:
                        s.gossip.observe(nid, r, c)
                if c:
                    client_addrs[nid] = c
            snapshot = dict(client_addrs)
        if s.store is not None:
            s.store.set_client_addrs(snapshot)

    sm = KVMachine.open(os.path.join(cfg.dir, "sm"), cfg.engine)
    tr = TCPTransport(raft_addrs, 1.0)
    rc = Config(id=cfg.id, peers=[p.id for p in cfg.peers], peer_info=infos, join=bool(cfg.join),
                on_config=on_config, dir=os.path.join(cfg.dir, "raft"), transport=tr, sm=sm,
                logf=cfg.logf)
    if cfg.tune is not None:
        cfg.tune(rc)
    s.transport = tr
    try:
        node = Node(rc)
    except BaseException:
        sm.close()
        tr.close()
        raise
    s.machine, s.node = sm, node
    try:
        me = Member(cfg.id, self_peer.raft_addr, self_peer.client_addr)
        s.gossip = Gossip(me, tr.gossip, seeds=[cfg.join] if cfg.join else [],
                          interval=cfg.gossip_interval)
        s.rpc = serve_rpc(self_peer.raft_addr, node, s.gossip.handle, s.is_dead)
        if cfg.join:
            _join_cluster(cfg, s)
        s.store = Store(node, sm, 0)
        with addrs_mu:
            s.store.set_client_addrs(dict(client_addrs))
        s.clients = kvnet.listen(self_peer.client_addr, s.store)
        node.start()  # reports the configuration through on_config
        s.gossip.start()
        s._reconciler = threading.Thread(target=s._reconcile_loop, name="reconcile", daemon=True)
        s._reconciler.start()
    except BaseException:
        s.close()
        raise
    return s


def _join_cluster(cfg: NodeConfig, s: Server) -> None:
    """Contact the seed, learn the cluster by gossip, copy its state. A seed
    that cannot be reached is not fatal: gossip keeps retrying it, and the
    leader will send the state once it adds this node."""
    deadline = time.monotonic() + cfg.join_timeout
    while not s.gossip.join(cfg.join):
        if time.monotonic() >= deadline:
            _log.warning("join: seed %s did not answer within %.1fs; retrying in the background",
                         cfg.join, cfg.join_timeout)
            return
        time.sleep(0.1)
    try:
        if s.node.bootstrap(lambda req: s.transport.pull_snapshot(cfg.join, req)):
            _log.info("join: copied the state of %s", cfg.join)
    except Exception as e:
        _log.warning("join: could not copy the state from %s (%s); the leader will send it", cfg.join, e)
