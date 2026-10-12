"""In-memory transport with partitions, a toy state machine and a cluster
harness for the Raft tests."""

import threading
import time

from kvstore.raft import LEADER, Config, KV, Node, StateMachine, Transport
from tests.helpers import TempDirTestCase


class MemNet:
    def __init__(self):
        self.mu = threading.Lock()
        self.nodes = {}
        self.blocked = set()  # nodes isolated from everyone

    def set(self, nid, node):
        with self.mu:
            self.nodes[nid] = node

    def remove(self, nid):
        with self.mu:
            self.nodes.pop(nid, None)

    def isolate(self, nid):
        with self.mu:
            self.blocked.add(nid)

    def heal(self, nid):
        with self.mu:
            self.blocked.discard(nid)

    def is_isolated(self, nid):
        with self.mu:
            return nid in self.blocked

    def route(self, frm, to):
        with self.mu:
            if frm in self.blocked or to in self.blocked:
                raise ConnectionError("partitioned")
            n = self.nodes.get(to)
            if n is None:
                raise ConnectionError("node down")
            return n


class MemTransport(Transport):
    def __init__(self, net, self_id):
        self.net, self.self_id = net, self_id

    def request_vote(self, to, req):
        return self.net.route(self.self_id, to).handle_request_vote(req)

    def append_entries(self, to, req):
        return self.net.route(self.self_id, to).handle_append_entries(req)

    def install_snapshot(self, to, req):
        return self.net.route(self.self_id, to).handle_install_snapshot(req)


class MemSM(StateMachine):
    """Toy state machine: commands are "k=v" (set) or "-k" (delete)."""

    def __init__(self):
        self.mu = threading.Lock()
        self.data = {}
        self.index = 0
        self.term = 0
        self.history = []  # every applied command, in order
        self.staging = None
        self.sync_limit = 0  # > 0 caps what sync reports as durable (a slow disk)

    def applied(self):
        with self.mu:
            return self.index, self.term

    def apply(self, index, term, cmds):
        with self.mu:
            for c in cmds:
                cmd = c.decode()
                self.history.append(cmd)
                if cmd.startswith("-"):
                    self.data.pop(cmd[1:], None)
                elif "=" in cmd:
                    k, v = cmd.split("=", 1)
                    self.data[k] = v
            self.index, self.term = index, term

    def sync(self):
        with self.mu:
            if self.sync_limit > 0 and self.index > self.sync_limit:
                return self.sync_limit
            return self.index

    def set_sync_limit(self, n):
        with self.mu:
            self.sync_limit = n

    def snapshot_scan(self, cursor, max_bytes):
        with self.mu:
            start = cursor.decode() if cursor else ""
            keys = sorted(k for k in self.data if k >= start)
            out, size = [], 0
            for i, k in enumerate(keys):
                if out and size >= max_bytes:
                    return out, keys[i].encode(), False
                out.append(KV(k.encode(), self.data[k].encode()))
                size += len(k) + len(self.data[k])
            return out, None, True

    def restore_begin(self):
        with self.mu:
            self.staging = {}

    def restore_chunk(self, items):
        with self.mu:
            for kv in items:
                self.staging[kv.key.decode()] = kv.value.decode()

    def restore_finish(self, index, term):
        with self.mu:
            self.data, self.staging, self.index, self.term = self.staging, None, index, term

    def restore_abort(self):
        with self.mu:
            self.staging = None

    def get(self, k):
        with self.mu:
            return self.data.get(k)

    def snapshot_data(self):
        with self.mu:
            return dict(self.data)

    def history_copy(self):
        with self.mu:
            return list(self.history)


def eventually(d, what, cond):
    end = time.monotonic() + d
    while time.monotonic() < end:
        if cond():
            return
        time.sleep(0.005)
    raise AssertionError("timed out waiting for %s" % what)


class ClusterTestCase(TempDirTestCase):
    def new_cluster(self, size, retain):
        self.net = MemNet()
        self.ids = ["n%d" % i for i in range(1, size + 1)]
        self.dirs = {i: self.tmpdir() for i in self.ids}
        self.sms = {i: MemSM() for i in self.ids}
        self.nodes = {}
        self.retain = retain
        for i in self.ids:
            self.start_node(i)
        self.addCleanup(self._stop_all)

    def _stop_all(self):
        for n in list(self.nodes.values()):
            n.stop()

    def start_node(self, nid):
        n = Node(Config(
            id=nid, peers=self.ids, dir=self.dirs[nid],
            transport=MemTransport(self.net, nid), sm=self.sms[nid],
            election_timeout=0.15, heartbeat_interval=0.02,
            log_retain=self.retain, snapshot_chunk_bytes=64, no_sync=True))
        self.nodes[nid] = n
        self.net.set(nid, n)
        n.start()

    def crash(self, nid):
        """Stop a node without touching its disk or state machine."""
        self.nodes.pop(nid).stop()
        self.net.remove(nid)

    def leader(self):
        ld = None
        for n in list(self.nodes.values()):
            st = n.status()
            if st.role == LEADER:
                if self.net.is_isolated(st.id):
                    continue
                if ld is not None and ld.status().term == st.term:
                    raise AssertionError("two leaders in term %d" % st.term)
                if ld is None or st.term > ld.status().term:
                    ld = n
        return ld

    def wait_leader(self):
        box = []

        def cond():
            ld = self.leader()
            if ld is not None:
                box[:] = [ld]
            return ld is not None

        eventually(5, "a leader", cond)
        return box[0]

    def propose(self, cmd):
        last = None
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            ld = self.leader()
            if ld is None:
                time.sleep(0.01)
                continue
            try:
                ld.propose(cmd.encode(), 2.0)
                return
            except Exception as e:
                last = e
            time.sleep(0.01)
        raise AssertionError("propose %r: %r" % (cmd, last))

    def wait_applied(self, ids, key, val):
        eventually(8, "%s to apply %s=%s" % (ids, key, val),
                   lambda: all(self.sms[i].get(key) == val for i in ids))
