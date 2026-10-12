"""kvserver serves the storage engine over TCP, either standalone or as a
member of a replicated (Raft) cluster.

    standalone: python3 -m kvstore.cmd.kvserver -addr 127.0.0.1:7000 -dir data
    cluster:    python3 -m kvstore.cmd.kvserver -id n1 -dir data1 \\
                  -peers n1=127.0.0.1:9001/127.0.0.1:7001,n2=127.0.0.1:9002/127.0.0.1:7002,n3=127.0.0.1:9003/127.0.0.1:7003
                (each peer is id=raftAddr/clientAddr; run one process per peer)
    new node:   python3 -m kvstore.cmd.kvserver -id n4 -dir data4 -addr 127.0.0.1:7004 \\
                  -raft 127.0.0.1:9004 -join 127.0.0.1:9001
                (-join is the raft address of ANY one member; the node finds the
                rest of the cluster by gossip, copies the data and is added by the leader.
                Without -join and -peers the same flags start a one-node cluster; a
                restart needs neither, membership is persisted.)
"""

import argparse
import logging
import signal
import sys
import threading

from .. import kvnet
from ..engine import DB, Options
from ..replica import NodeConfig, Peer, parse_peers, start_server
from ..util import parse_duration

log = logging.getLogger("kvserver")


def wait_for_signal() -> None:
    ev = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: ev.set())
    while not ev.wait(0.5):
        pass


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="kvserver", description=__doc__.split("\n")[0])
    ap.add_argument("-addr", "--addr", default="127.0.0.1:7000", help="client listen address (standalone mode)")
    ap.add_argument("-dir", "--dir", default="data", help="data directory")
    ap.add_argument("-nosync", "--nosync", action="store_true", help="disable engine fsync (faster, not crash-safe)")
    ap.add_argument("-id", "--id", default="", help="this node's ID (cluster mode)")
    ap.add_argument("-peers", "--peers", default="",
                    help="cluster members: id=raftAddr/clientAddr,... (enables cluster mode)")
    ap.add_argument("-raft", "--raft", default="", help="this node's raft RPC address (with -join)")
    ap.add_argument("-join", "--join", default="",
                    help="raft address of one existing member: join its cluster as a new node")
    ap.add_argument("-stats", "--stats", default="0",
                    help="log engine statistics at this interval (standalone mode), e.g. 5s")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stderr)

    if not args.peers and not args.join and not args.id:
        db = DB.open(args.dir, Options(no_sync=args.nosync))
        srv = kvnet.listen(args.addr, db)
        log.info("standalone: serving %s on %s", args.dir, srv.addr())
        every = parse_duration(args.stats)
        stop = threading.Event()
        if every > 0:
            def report():
                while not stop.wait(every):
                    log.info("stats: %s", db.stats())

            threading.Thread(target=report, daemon=True).start()
        wait_for_signal()
        stop.set()
        srv.close()
        db.close()
        return 0

    if not args.peers:
        # -id [-raft] [-join]: a one-node cluster (fresh directory, no -join), a new node
        # joining a running cluster, or a restart of either (membership is persisted).
        if not args.id or not args.raft:
            ap.error("-id and -raft (plus -addr, the client address) are needed without -peers")
        peers = [Peer(args.id, args.raft, args.addr)]
    else:
        peers = parse_peers(args.peers)
    s = start_server(NodeConfig(
        id=args.id, dir=args.dir, peers=peers, join=args.join,
        engine=Options(no_sync=args.nosync),
        tune=lambda rc: setattr(rc, "no_sync", args.nosync),  # -nosync also skips the Raft log fsync
        logf=lambda f, *a: log.info(f % a if a else f)))
    log.info("cluster node %s: clients on %s", args.id, s.client_addr())
    wait_for_signal()
    s.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
