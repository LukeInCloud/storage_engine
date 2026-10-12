import logging
import os
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from .. import kvnet
from ..engine import Options
from ..raft import Config, Node, TCPTransport, serve_rpc
from .machine import KVMachine
from .store import Store


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
    peers: List[Peer]  # whole cluster, including this node
    engine: Options = field(default_factory=Options)
    tune: Optional[Callable[[Config], None]] = None  # optional tweaks (timeouts, retention, ...)
    logf: Optional[Callable] = None


class Server:
    """One running cluster member."""

    def __init__(self):
        self.machine = None
        self.node = None
        self.store = None
        self.rpc = None
        self.transport = None
        self.clients = None

    def client_addr(self) -> str:
        return self.clients.addr()

    def close(self) -> None:
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


def start_server(cfg: NodeConfig) -> Server:
    self_peer = None
    ids = []
    raft_addrs, client_addrs = {}, {}
    for p in cfg.peers:
        ids.append(p.id)
        raft_addrs[p.id], client_addrs[p.id] = p.raft_addr, p.client_addr
        if p.id == cfg.id:
            self_peer = p
    if self_peer is None:
        raise ValueError("replica: node %r is not in the peer list" % cfg.id)

    sm = KVMachine.open(os.path.join(cfg.dir, "sm"), cfg.engine)
    tr = TCPTransport(raft_addrs, 1.0)
    rc = Config(id=cfg.id, peers=ids, dir=os.path.join(cfg.dir, "raft"), transport=tr, sm=sm,
                logf=cfg.logf)
    if cfg.tune is not None:
        cfg.tune(rc)
    try:
        node = Node(rc)
    except BaseException:
        sm.close()
        raise
    s = Server()
    s.machine, s.node, s.transport = sm, node, tr
    try:
        s.rpc = serve_rpc(self_peer.raft_addr, node)
        s.store = Store(node, sm, 0)
        s.store.set_client_addrs(client_addrs)
        s.clients = kvnet.listen(self_peer.client_addr, s.store)
        node.start()
    except BaseException:
        s.close()
        raise
    return s
