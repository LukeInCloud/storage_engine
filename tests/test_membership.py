"""Dynamic membership: gossip, learner/voter changes in Raft, bootstrapping a
new node from a peer, all the way through real TCP nodes."""

import os
import threading
import time
import unittest

from kvstore.engine import Options
from kvstore.gossip import Gossip, Member
from kvstore.gossip.gossip import decode_table, encode_table
from kvstore.raft import LEADER, Membership, PullReq
from kvstore.raft.membership import load_base, save_base
from kvstore.replica import NodeConfig, Peer, start_server
from tests.helpers import TempDirTestCase, wait_for
from tests.raft_harness import ClusterTestCase, MemSM, MemTransport, eventually
from tests.test_replica import TCluster, free_addr
from kvstore.raft import Config, Node


class TestMembershipType(TempDirTestCase):
    def test_round_trip_and_rules(self):
        m = Membership({"a": "x/y", "b": "", "c": "z"}, ["a", "c"])
        self.assertEqual(Membership.decode(m.encode()), m)
        self.assertEqual(m.learners(), ["b"])
        self.assertEqual(m.with_voter("b").voters, {"a", "b", "c"})
        with self.assertRaises(ValueError):
            Membership({"a": ""}, ["zzz"])
        for bad in (b"\x05", b"\x01\x09abc"):
            with self.assertRaises(ValueError):
                Membership.decode(bad)

    def test_base_file(self):
        d = self.tmpdir()
        self.assertIsNone(load_base(d))
        m = Membership({"a": "1", "b": "2"}, ["a"])
        save_base(d, 17, m)
        self.assertEqual(load_base(d), (17, m))
        path = os.path.join(d, "members")
        with open(path, "rb") as f:
            raw = bytearray(f.read())
        raw[-1] ^= 0xFF
        with open(path, "wb") as f:
            f.write(raw)
        with self.assertRaises(ValueError):
            load_base(d)


class GossipNet:
    """In-memory exchange between Gossip objects, addressed by raft_addr."""

    def __init__(self):
        self.nodes = {}
        self.down = set()

    def add(self, g):
        self.nodes[g.me.raft_addr] = g

    def exchange(self, addr, body, timeout):
        g = self.nodes.get(addr)
        if g is None or addr in self.down:
            raise ConnectionError("down")
        return g.handle(body)


class TestGossip(unittest.TestCase):
    def make(self, net, nid, seeds=(), **kw):
        g = Gossip(Member(nid, "r-" + nid, "c-" + nid), net.exchange, seeds=seeds,
                   interval=0.02, timeout=0.2, **kw)
        net.add(g)
        return g

    def test_table_encoding(self):
        t = [Member("a", "r", "c", 5, 9), Member("b", "", "", 0, 0)]
        self.assertEqual(decode_table(encode_table(t)), t)
        for bad in (b"\x01", b"\x01\x05ab", b"\x02\x00\x00\x00\x00\x00"):
            with self.assertRaises(ValueError):
                decode_table(bad)

    def test_one_seed_is_enough(self):
        net = GossipNet()
        gs = [self.make(net, "n1")]
        for i in range(2, 9):
            gs.append(self.make(net, "n%d" % i, seeds=["r-n1"]))  # everybody knows only n1
        for g in gs:
            g.start()
        self.addCleanup(lambda: [g.close() for g in gs])
        want = {"n%d" % i for i in range(1, 9)}
        eventually(5, "full tables", lambda: all({m.id for m in g.members()} == want for g in gs))
        # A late joiner knows only n5 and is known to everybody soon after.
        late = self.make(net, "n9", seeds=["r-n5"])
        late.start()
        self.addCleanup(late.close)
        want.add("n9")
        eventually(5, "n9 spread", lambda: all({m.id for m in g.members()} == want for g in gs + [late]))

    def test_liveness_and_restart(self):
        net = GossipNet()
        a = self.make(net, "a", fail_after=0.3)
        b = self.make(net, "b", seeds=["r-a"], fail_after=0.3)
        a.start()
        b.start()
        self.addCleanup(a.close)
        eventually(3, "both alive", lambda: all(m.alive for m in a.members()) and len(a.members()) == 2)
        b.close()
        eventually(3, "b suspected", lambda: not next(m for m in a.members() if m.id == "b").alive)
        # b restarts (heartbeat back to 0, new generation): it must be alive again.
        time.sleep(0.01)
        b2 = Gossip(Member("b", "r-b", "c-b"), net.exchange, seeds=["r-a"], interval=0.02, timeout=0.2)
        net.add(b2)
        b2.start()
        self.addCleanup(b2.close)
        eventually(3, "b alive again", lambda: next(m for m in a.members() if m.id == "b").alive)

    def test_address_change_is_reported(self):
        net = GossipNet()
        seen = []
        a = self.make(net, "a", on_change=seen.append)
        b = self.make(net, "b", seeds=["r-a"])
        a.start()
        b.start()
        self.addCleanup(a.close)
        eventually(3, "a learns b", lambda: any(m.id == "b" for m in seen))
        b.close()
        time.sleep(0.01)
        b2 = Gossip(Member("b", "r-b", "c-b-moved"), net.exchange, seeds=["r-a"], interval=0.02, timeout=0.2)
        net.add(b2)
        b2.start()
        self.addCleanup(b2.close)
        eventually(3, "new client address", lambda: any(m.client_addr == "c-b-moved" for m in seen))


class TestRaftMembership(ClusterTestCase):
    def start_joiner(self, nid, retain):
        self.dirs[nid] = self.tmpdir()
        self.sms[nid] = MemSM()
        n = Node(Config(
            id=nid, join=True, dir=self.dirs[nid], transport=MemTransport(self.net, nid),
            sm=self.sms[nid], election_timeout=0.15, heartbeat_interval=0.02,
            log_retain=retain, snapshot_chunk_bytes=64, no_sync=True))
        self.nodes[nid] = n
        self.net.set(nid, n)
        n.start()
        return n

    def voters_everywhere(self, ids, want):
        return all(self.nodes[i].membership().voters == frozenset(want) for i in ids)

    def test_learner_is_promoted_after_catching_up(self):
        self.new_cluster(3, retain=1000)
        for i in range(30):
            self.propose("k%d=%d" % (i, i))
        j = self.start_joiner("n4", 1000)
        time.sleep(0.5)
        # Not a member: it neither campaigns nor gets anything yet.
        self.assertEqual(j.status().term, 0)
        self.assertEqual(j.membership().members, {})
        ld = self.wait_leader()
        eventually(5, "n4 added", lambda: ld.add_member("n4", "info-4"))
        self.assertFalse(ld.add_member("n4", "info-4"), "already a member")
        want = ["n1", "n2", "n3", "n4"]
        eventually(8, "n4 promoted everywhere", lambda: self.voters_everywhere(want, want))
        self.assertEqual(self.nodes["n4"].membership().members["n4"], "info-4")
        eventually(8, "n4 has the data", lambda: self.sms["n4"].snapshot_data() == self.sms["n1"].snapshot_data())
        self.propose("after=1")
        self.wait_applied(want, "after", "1")

    def test_new_node_can_win_elections_and_commit(self):
        self.new_cluster(3, retain=1000)
        self.start_joiner("n4", 1000)
        eventually(5, "n4 added", lambda: self.wait_leader().add_member("n4"))
        want = ["n1", "n2", "n3", "n4"]
        eventually(8, "n4 voter", lambda: self.voters_everywhere(want, want))
        # With 4 voters the quorum is 3: take two old nodes down; progress needs n4.
        self.propose("a=1")
        self.crash("n1")
        self.propose("b=2")
        self.wait_applied(["n2", "n3", "n4"], "b", "2")
        self.crash("n2")  # only n3 and n4 left: no quorum of 4
        time.sleep(0.5)
        # No CheckQuorum, so a stranded leader may keep its role; it must not commit.
        for n in self.nodes.values():
            with self.assertRaises(Exception):
                n.propose(b"c=3", 0.5)

    def test_snapshot_carries_the_configuration(self):
        """The joiner gets its state by snapshot (log compacted); the
        configuration must come with it, also across a restart."""
        self.new_cluster(3, retain=5)
        for i in range(60):
            self.propose("k%d=%d" % (i, i))
        eventually(8, "compaction", lambda: self.wait_leader().status().log_start > 20)
        self.start_joiner("n4", 5)
        eventually(5, "n4 added", lambda: self.wait_leader().add_member("n4", "i4"))
        want = ["n1", "n2", "n3", "n4"]
        eventually(8, "n4 voter", lambda: self.voters_everywhere(want, want))
        eventually(8, "data", lambda: self.sms["n4"].snapshot_data() == self.sms["n1"].snapshot_data())
        # Restart n4 as a plain member from its own disk (no join flag, no peers).
        self.nodes.pop("n4").stop()
        self.net.remove("n4")
        n = Node(Config(id="n4", peers=["n4"], dir=self.dirs["n4"], transport=MemTransport(self.net, "n4"),
                        sm=self.sms["n4"], election_timeout=0.15, heartbeat_interval=0.02,
                        log_retain=5, snapshot_chunk_bytes=64, no_sync=True))
        self.nodes["n4"] = n
        self.net.set("n4", n)
        self.assertEqual(n.membership().voters, frozenset(want))
        n.start()
        self.propose("post=1")
        self.wait_applied(want, "post", "1")

    def test_bootstrap_from_a_follower(self):
        self.new_cluster(3, retain=1000)
        for i in range(40):
            self.propose("k%d=%d" % (i, i))
        ld = self.wait_leader()
        seed = next(n for n in self.nodes.values() if n is not ld)
        eventually(5, "seed applied", lambda: seed.status().applied >= ld.status().commit)
        self.dirs["n4"], self.sms["n4"] = self.tmpdir(), MemSM()
        j = Node(Config(id="n4", join=True, dir=self.dirs["n4"], transport=MemTransport(self.net, "n4"),
                        sm=self.sms["n4"], election_timeout=0.15, heartbeat_interval=0.02,
                        snapshot_chunk_bytes=64, no_sync=True))
        self.addCleanup(j.stop)
        self.assertTrue(j.bootstrap(seed.handle_pull_snapshot))
        self.assertEqual(self.sms["n4"].snapshot_data(), self.sms["n1"].snapshot_data())
        self.assertGreater(j.status().applied, 0)
        self.assertEqual(j.membership().voters, frozenset(["n1", "n2", "n3"]))
        self.assertFalse(j.bootstrap(seed.handle_pull_snapshot), "only a fresh node may bootstrap")
        # It then joins normally, and only the tail is replicated.
        self.nodes["n4"] = j
        self.net.set("n4", j)
        j.start()
        eventually(5, "n4 added", lambda: ld.add_member("n4"))
        want = ["n1", "n2", "n3", "n4"]
        eventually(8, "voter", lambda: self.voters_everywhere(want, want))
        self.propose("tail=1")
        self.wait_applied(want, "tail", "1")

    def test_one_change_at_a_time_and_only_on_the_leader(self):
        self.new_cluster(3, retain=1000)
        ld = self.wait_leader()
        follower = next(n for n in self.nodes.values() if n is not ld)
        with self.assertRaises(Exception):
            follower.add_member("x")
        self.start_joiner("n4", 1000)
        self.start_joiner("n5", 1000)
        eventually(5, "n4 added", lambda: ld.add_member("n4"))
        # n5 may only be added once n4's entry is committed; retrying gets there.
        eventually(5, "second change accepted", lambda: ld.add_member("n5"))
        want = ["n1", "n2", "n3", "n4", "n5"]
        eventually(10, "five voters", lambda: self.voters_everywhere(want, want))


class TestJoinCluster(TempDirTestCase):
    def join(self, c, nid, seed_id, **kw):
        seed = next(p for p in c.peers + list(getattr(c, "joined", {}).values()) if p.id == seed_id)
        me = Peer(nid, free_addr(), free_addr())
        c.dirs[nid] = self.tmpdir()
        c.joined = getattr(c, "joined", {})
        c.joined[nid] = me

        def tune(rc):
            rc.election_timeout = 0.2
            rc.heartbeat_interval = 0.025
            rc.no_sync = True
            if c.tune:
                c.tune(rc)

        c.servers[nid] = start_server(NodeConfig(
            id=nid, dir=c.dirs[nid], peers=[me], join=seed.raft_addr, engine=Options(no_sync=True),
            tune=tune, gossip_interval=0.1, logf=lambda f, *a: c.logs.append(f % a), **kw))
        return c.servers[nid]

    def all_voters(self, c, ids):
        return all(c.servers[i].node.membership().voters == frozenset(ids) for i in ids)

    def test_new_node_knows_one_peer(self):
        c = TCluster(self, 3)
        c.wait_leader()
        cl = c.client()
        want = {}
        for i in range(40):
            k, v = "key-%03d" % i, "val-%d" % i
            cl.put(k.encode(), v.encode())
            want[k] = v
        c.wait_converged(want)

        # n4 is told about n2 only (maybe a follower; it does not matter).
        s4 = self.join(c, "n4", "n2")
        ids = ["n1", "n2", "n3", "n4"]
        # 1. It learned the whole cluster by gossip, and everybody learned about it.
        wait_for(lambda: all({m.id for m in c.servers[i].members()} == set(ids) for i in ids),
                 10, "gossip to converge")
        # 2. It holds the data (copied from the peer / replicated by the leader).
        c.wait_converged(want, ids)
        self.assertTrue(any("bootstrapped from a peer" in l for l in c.logs))
        # 3. The leader made it a voting member of Raft.
        wait_for(lambda: self.all_voters(c, ids), 10, "n4 to become a voter")
        # 4. It takes part: writes reach it, it can serve reads once leader.
        cl.put(b"later", b"1")
        want["later"] = "1"
        c.wait_converged(want, ids)
        # 5. Cluster keeps working with the old leader gone; n4 may even lead.
        old = c.leader_id()
        c.stop(old)
        cl.put(b"after-kill", b"1")
        want["after-kill"] = "1"
        c.wait_converged(want, [i for i in ids if i != old])

    def test_chain_of_joins_and_restart_of_a_joined_node(self):
        c = TCluster(self, 1)  # a single node bootstraps the cluster
        c.wait_leader()
        cl = c.client()
        cl.put(b"seed", b"data")
        self.join(c, "n2", "n1")
        wait_for(lambda: self.all_voters(c, ["n1", "n2"]), 10, "n2 voter")
        self.join(c, "n3", "n2")  # knows only n2
        wait_for(lambda: self.all_voters(c, ["n1", "n2", "n3"]), 15, "n3 voter")
        self.join(c, "n4", "n3")  # knows only n3
        ids = ["n1", "n2", "n3", "n4"]
        wait_for(lambda: self.all_voters(c, ids), 15, "n4 voter")
        want = {"seed": "data"}
        for i in range(20):
            cl.put(b"k%d" % i, b"v%d" % i)
            want["k%d" % i] = "v%d" % i
        c.wait_converged(want, ids)

        # Restart n4 with no join flag: membership comes from its own disk.
        c.servers.pop("n4").close()
        me = c.joined["n4"]
        c.servers["n4"] = start_server(NodeConfig(
            id="n4", dir=c.dirs["n4"], peers=[me], engine=Options(no_sync=True),
            tune=lambda rc: (setattr(rc, "election_timeout", 0.2), setattr(rc, "heartbeat_interval", 0.025),
                             setattr(rc, "no_sync", True)),
            gossip_interval=0.1))
        self.assertEqual(c.servers["n4"].node.membership().voters, frozenset(ids))
        cl.put(b"final", b"1")
        want["final"] = "1"
        c.wait_converged(want, ids)

    def test_join_with_unreachable_seed_keeps_trying(self):
        c = TCluster(self, 1)
        c.wait_leader()
        seed = c.peers[0]
        c.stop("n1")
        me = Peer("n2", free_addr(), free_addr())
        d = self.tmpdir()
        s = start_server(NodeConfig(
            id="n2", dir=d, peers=[me], join=seed.raft_addr, join_timeout=0.3,
            engine=Options(no_sync=True), gossip_interval=0.1,
            tune=lambda rc: setattr(rc, "no_sync", True)))
        self.addCleanup(s.close)
        # Not part of anything: it must not have elected itself.
        time.sleep(0.8)
        self.assertNotEqual(s.node.status().role, LEADER)
        self.assertEqual(s.node.membership().voters, frozenset())
        # The seed comes back (same dir, same addresses): n2 finds it by itself.
        c.start("n1")
        wait_for(lambda: s.node.membership().voters == frozenset(["n1", "n2"]), 15, "n2 to be added")


if __name__ == "__main__":
    unittest.main()
