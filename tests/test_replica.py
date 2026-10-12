import os
import random
import socket
import threading
import time
import unittest

from kvstore import kvnet
from kvstore.engine import DB, NotFoundError, Op, Options
from kvstore.raft import LEADER, KV
from kvstore.replica import (APPLIED_KEY, KVMachine, NodeConfig, Peer, decode_command,
                             encode_applied, encode_command, start_server)
from tests.helpers import TempDirTestCase, short, wait_for


def open_machine(d):
    return KVMachine.open(d, Options(no_sync=True))


class TestMachine(TempDirTestCase):
    def test_command_round_trip(self):
        ops = [Op(b"a", b"1"), Op(b"b", b"", True), Op(b"c", b"")]  # c: empty value
        got = decode_command(encode_command(ops))
        self.assertEqual(len(got), 3)
        self.assertEqual((got[0].key, got[0].value, got[1].delete, got[2].delete),
                         (b"a", b"1", True, False))
        for bad in (b"", b"\xff", bytes([5, 1]), encode_command(ops)[:6]):
            with self.assertRaises(Exception):
                decode_command(bad)

    def test_applied_marker_is_atomic_with_data(self):
        d = self.tmpdir()
        m = open_machine(d)
        self.assertEqual(m.applied()[0], 0)
        m.apply(7, 3, [encode_command([Op(b"k", b"v")])])
        m.close()
        m = open_machine(d)
        self.addCleanup(m.close)
        self.assertEqual(m.applied(), (7, 3))
        self.assertEqual(m.get(b"k"), b"v")
        self.assertEqual(len(m.range(None, None)), 1, "internal marker leaked into range")
        items, _, done = m.snapshot_scan(None, 1 << 20)
        self.assertTrue(done)
        self.assertEqual(len(items), 1, "snapshot must exclude the marker")

    def test_restore_swaps_state_atomically(self):
        m = open_machine(self.tmpdir())
        self.addCleanup(m.close)
        m.apply(5, 1, [encode_command([Op(b"old", b"x")])])
        m.restore_begin()
        m.restore_chunk([KV(b"new1", b"a")])
        m.restore_chunk([KV(b"new2", b"b")])
        self.assertEqual(m.get(b"old"), b"x", "live state must be untouched until finish")
        m.restore_finish(42, 9)
        with self.assertRaises(NotFoundError):
            m.get(b"old")
        self.assertEqual(m.get(b"new2"), b"b")
        self.assertEqual(m.applied(), (42, 9))

    def test_aborted_restore_leaves_state_intact(self):
        d = self.tmpdir()
        m = open_machine(d)
        m.apply(1, 1, [encode_command([Op(b"keep", b"1")])])
        m.restore_begin()
        m.restore_chunk([KV(b"junk", b"1")])
        m.restore_abort()
        m.close()
        m = open_machine(d)
        self.addCleanup(m.close)
        with self.assertRaises(NotFoundError):
            m.get(b"junk")
        self.assertEqual(m.get(b"keep"), b"1")
        self.assertFalse(os.path.exists(os.path.join(d, "db.restore")), "staging dir not cleaned")

    def test_interrupted_restore_is_completed_on_open(self):
        """Crash right after the commit point: the next open must complete the swap."""
        d = self.tmpdir()
        m = open_machine(d)
        m.apply(1, 1, [encode_command([Op(b"old", b"1")])])
        m.close()
        # Build a finished staging db by hand and drop the ready flag, as a crash would.
        st = DB.open(os.path.join(d, "db.restore"), Options(no_sync=True))
        st.write([Op(b"fresh", b"1"), Op(APPLIED_KEY, encode_applied(99, 4))])
        st.close()
        open(os.path.join(d, "restore.ready"), "wb").close()

        m = open_machine(d)
        self.addCleanup(m.close)
        self.assertEqual(m.get(b"fresh"), b"1", "swap was not completed")
        with self.assertRaises(NotFoundError):
            m.get(b"old")
        self.assertEqual(m.applied()[0], 99)


def free_addr() -> str:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    addr = "127.0.0.1:%d" % s.getsockname()[1]
    s.close()
    return addr


class TCluster:
    """A real cluster: TCP, real engine, one directory per node."""

    def __init__(self, tc: TempDirTestCase, size: int, tune=None, node_kw=None):
        self.tc = tc
        self.node_kw = node_kw or {}
        self.tune = tune
        self.peers = [Peer("n%d" % i, free_addr(), free_addr()) for i in range(1, size + 1)]
        self.dirs = {p.id: tc.tmpdir() for p in self.peers}
        self.servers = {}
        self.mu = threading.Lock()
        self.logs = []
        for p in self.peers:
            self.start(p.id)
        tc.addCleanup(self.close_all)

    def close_all(self):
        for s in list(self.servers.values()):
            s.close()
        self.servers.clear()

    def start(self, nid):
        def tune(rc):
            rc.election_timeout = 0.2
            rc.heartbeat_interval = 0.025
            rc.no_sync = True
            if self.tune:
                self.tune(rc)

        def logf(f, *a):
            with self.mu:
                self.logs.append(f % a)

        self.servers[nid] = start_server(NodeConfig(
            id=nid, dir=self.dirs[nid], peers=self.peers,
            engine=Options(no_sync=True), tune=tune, logf=logf, **self.node_kw))

    def stop(self, nid):
        self.servers.pop(nid).close()

    def logged(self, substr):
        with self.mu:
            return any(substr in l for l in self.logs)

    def client_addrs(self):
        return [p.client_addr for p in self.peers]

    def client(self):
        cl = kvnet.dial_cluster(self.client_addrs(), 3.0, 20.0)
        self.tc.addCleanup(cl.close)
        return cl

    def leader_id(self):
        for nid, s in list(self.servers.items()):
            if s.node.status().role == LEADER:
                return nid
        return ""

    def wait_leader(self):
        found = [""]

        def cond():
            found[0] = self.leader_id()
            return bool(found[0])

        wait_for(cond, 10, "a leader")
        return found[0]

    def wait_converged(self, want: dict, ids=None):
        ids = list(ids) if ids else list(self.servers)

        def ok():
            for nid in ids:
                try:
                    kvs = self.servers[nid].machine.range(None, None)
                except Exception:
                    return False
                if len(kvs) != len(want) or any(want.get(kv.key.decode()) != kv.value.decode() for kv in kvs):
                    return False
            return True

        wait_for(ok, 15, "replicas to converge")


class TestCluster(TempDirTestCase):
    def test_all_operations(self):
        c = TCluster(self, 3)
        c.wait_leader()
        cl = c.client()
        with self.assertRaises(NotFoundError):
            cl.read(b"nope")
        cl.put(b"a", b"1")
        cl.batch_put([b"b", b"c", b"d"], [b"2", b"3", b"4"])
        cl.delete(b"c")
        self.assertEqual(cl.read(b"b"), b"2")
        self.assertEqual(len(cl.read_key_range(b"a", b"d")), 3)
        # Every replica holds the data; internal bookkeeping keys stay hidden.
        c.wait_converged({"a": "1", "b": "2", "d": "4"})
        with self.assertRaises(Exception):
            cl.put(b"\x00raft/applied", b"x")  # reserved key must be rejected
        with self.assertRaises(Exception):
            cl.put(b"", b"x")

    def test_cli_follows_leader_from_any_single_address(self):
        import contextlib
        import io
        from kvstore.cmd import kvcli

        c = TCluster(self, 3)
        lid = c.wait_leader()
        follower = next(p for p in c.peers if p.id != lid)

        def run(*cmd):
            raw = io.BytesIO()
            out = io.TextIOWrapper(raw, write_through=True)  # the CLI writes to .buffer
            with contextlib.redirect_stdout(out):
                rc = kvcli.main(["-addr", follower.client_addr, *cmd])
            return rc, raw.getvalue().decode()

        self.assertEqual(run("put", "city", "Warsaw")[0], 0)  # a follower's address
        self.assertEqual(run("get", "city"), (0, "Warsaw\n"))
        self.assertEqual(run("get", "nope")[0], 1)

    def test_leader_failover_and_rejoin(self):
        c = TCluster(self, 3)
        c.wait_leader()
        cl = c.client()
        want = {}

        def put(i):
            k, v = "key-%04d" % i, "val-%d" % i
            cl.put(k.encode(), v.encode())
            want[k] = v

        for i in range(30):
            put(i)
        dead = c.leader_id()
        c.stop(dead)
        # The same client keeps working: it fails over to the new leader by itself.
        for i in range(30, 60):
            put(i)
        self.assertEqual(cl.read(b"key-0007"), b"val-7")
        nl = c.leader_id()
        self.assertTrue(nl and nl != dead)
        c.wait_converged(want, list(c.servers))
        # The old leader comes back and catches up from its own disk + the log.
        c.start(dead)
        c.wait_converged(want)

    def test_whole_restart(self):
        c = TCluster(self, 3)
        c.wait_leader()
        cl = c.client()
        want = {}
        for i in range(25):
            k, v = "k%03d" % i, "v%d" % i
            cl.put(k.encode(), v.encode())
            want[k] = v
        cl.delete(b"k005")
        del want["k005"]
        c.wait_converged(want)
        for p in c.peers:
            c.stop(p.id)
        for p in c.peers:
            c.start(p.id)
        c.wait_leader()
        c.wait_converged(want)
        cl2 = c.client()
        self.assertEqual(cl2.read(b"k024"), b"v24")
        cl2.put(b"new", b"1")

    def test_snapshot_catch_up_with_real_engine(self):
        def tune(rc):
            rc.log_retain = 20
            rc.snapshot_chunk_bytes = 4 << 10

        c = TCluster(self, 3, tune)
        c.wait_leader()
        cl = c.client()
        lagger = next(nid for nid in c.servers if nid != c.leader_id())
        c.stop(lagger)

        want = {}
        val = "x" * 200
        for i in range(250):
            k, v = "key-%04d" % i, "%d-%s" % (i, val)
            cl.put(k.encode(), v.encode())
            want[k] = v
            if i % 7 == 0:
                dk = "key-%04d" % (i // 2)
                cl.delete(dk.encode())
                want.pop(dk, None)
        wait_for(lambda: c.servers[c.leader_id()].node.status().log_start > 100, 10, "leader log compaction")
        c.start(lagger)
        # Writes during the transfer exercise the fuzzy-snapshot repair.
        for i in range(250, 290):
            k, v = "key-%04d" % i, "late-%d" % i
            cl.put(k.encode(), v.encode())
            want[k] = v
        c.wait_converged(want)
        self.assertTrue(c.logged("installed snapshot"), "the lagging node should have used a snapshot")
        # It is a normal, writable member afterwards.
        cl.put(b"final", b"1")
        want["final"] = "1"
        c.wait_converged(want)

    def test_linearizable_under_failover(self):
        """A single client doing read-your-writes must never observe stale
        data, not even while the leader is killed under it."""
        c = TCluster(self, 3)
        c.wait_leader()
        cl = c.client()
        killed = False
        for i in range(1, 81):
            v = b"%d" % i
            cl.put(b"counter", v)
            self.assertEqual(cl.read(b"counter"), v, "after put %d" % i)
            if i == 40 and not killed:
                c.stop(c.leader_id())
                killed = True

    def test_concurrent_clients(self):
        c = TCluster(self, 3)
        c.wait_leader()
        want = {}
        mu = threading.Lock()
        errors = []

        def worker(w):
            try:
                cl = c.client()
                for i in range(40):
                    k, v = "w%d-%03d" % (w, i), "v%d" % i
                    cl.put(k.encode(), v.encode())
                    with mu:
                        want[k] = v
            except Exception as e:  # noqa
                errors.append(e)

        ts = [threading.Thread(target=worker, args=(w,)) for w in range(6)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errors, [])
        c.wait_converged(want)
        self.assertEqual(c.servers["n1"].machine.range(None, None), c.servers["n2"].machine.range(None, None))


@unittest.skipIf(short(), "slow")
class TestChaos(TempDirTestCase):
    def test_chaos_kill_restart_nodes(self):
        """Writers hammer the cluster while nodes are repeatedly stopped and
        restarted (one at a time, so a majority always exists). Every
        acknowledged write must be present on every replica at the end."""
        def tune(rc):
            rc.log_retain = 200  # also exercise snapshots when a node returns
            rc.snapshot_chunk_bytes = 8 << 10

        c = TCluster(self, 3, tune)
        c.wait_leader()
        mu = threading.Lock()
        want = {}
        failures = []
        stop = threading.Event()

        def writer(w):
            cl = c.client()
            i = 0
            while not stop.is_set():
                k, v = "w%d-%06d" % (w, i), "value-%d-%d" % (w, i)
                try:
                    cl.put(k.encode(), v.encode())
                except Exception as e:
                    failures.append("put %s failed after retries: %r" % (k, e))
                    return
                with mu:
                    want[k] = v
                if i % 9 == 0:  # reads must be consistent with acknowledged writes
                    try:
                        got = cl.read(k.encode())
                    except Exception as e:
                        failures.append("read %s: %r" % (k, e))
                        return
                    if got.decode() != v:
                        failures.append("read-your-write %s = %r" % (k, got))
                        return
                i += 1

        ts = [threading.Thread(target=writer, args=(w,)) for w in range(3)]
        for t in ts:
            t.start()
        rnd = random.Random(time.time_ns())
        end = time.monotonic() + 10
        kills = 0
        while time.monotonic() < end and not failures:
            victim = rnd.choice(c.peers).id
            c.stop(victim)
            kills += 1
            time.sleep(0.2 + rnd.random() * 0.7)
            c.start(victim)
            time.sleep(0.3 + rnd.random() * 0.5)
        stop.set()
        for t in ts:
            t.join()
        self.assertEqual(failures, [])
        with mu:
            snapshot = dict(want)
        self.assertGreaterEqual(len(snapshot), 50, "too few writes to be meaningful")
        c.wait_converged(snapshot)


if __name__ == "__main__":
    unittest.main()
