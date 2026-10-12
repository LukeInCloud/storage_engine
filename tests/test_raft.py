import threading
import time
import unittest

from kvstore.raft import FOLLOWER, LEADER, NotLeaderError
from tests.raft_harness import ClusterTestCase, eventually


class TestRaft(ClusterTestCase):
    def test_single_node_cluster(self):
        self.new_cluster(1, 100)
        self.wait_leader()
        self.propose("a=1")
        self.wait_applied(self.ids, "a", "1")
        self.nodes["n1"].read_barrier(1.0)

    def test_elects_exactly_one_leader_and_replicates(self):
        self.new_cluster(3, 100)
        self.wait_leader()
        for i in range(20):
            self.propose("k%d=v%d" % (i, i))
        self.wait_applied(self.ids, "k19", "v19")
        # Followers reject proposals with a leader hint.
        for nid in self.ids:
            n = self.nodes[nid]
            if n.status().role == LEADER:
                continue
            with self.assertRaises(NotLeaderError):
                n.propose(b"x=1", 1.0)

    def test_all_nodes_apply_same_order(self):
        self.new_cluster(3, 1000)
        self.wait_leader()
        errors = []

        def worker(w):
            try:
                for i in range(25):
                    self.propose("w%d-%d=x" % (w, i))
            except Exception as e:  # noqa
                errors.append(e)

        ts = [threading.Thread(target=worker, args=(w,)) for w in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errors, [])
        self.wait_applied(self.ids, "w3-24", "x")

        def same():
            hs = [self.sms[i].history_copy() for i in self.ids]
            return all(h == hs[0] for h in hs) and len(hs[0]) == 100

        eventually(5, "equal histories", same)

    def test_leader_failover(self):
        self.new_cluster(3, 100)
        old = self.wait_leader()
        for i in range(10):
            self.propose("before%d=1" % i)
        old_id = old.status().id
        self.crash(old_id)
        rest = [i for i in self.ids if i != old_id]
        nl = self.wait_leader()
        self.assertNotEqual(nl.status().id, old_id)
        self.propose("after=1")
        self.wait_applied(rest, "after", "1")
        for i in range(10):
            self.wait_applied(rest, "before%d" % i, "1")  # committed data survived
        # The old leader restarts from its disk and catches up as a follower.
        self.start_node(old_id)
        self.wait_applied([old_id], "after", "1")
        st = self.nodes[old_id].status()
        self.assertFalse(st.role == LEADER and st.term == nl.status().term,
                         "restarted node must not displace the leader")

    def test_no_progress_without_majority(self):
        self.new_cluster(3, 100)
        ld = self.wait_leader()
        lid = ld.status().id
        for other in self.ids:
            if other != lid:
                self.net.isolate(other)
        with self.assertRaises(Exception):
            ld.propose(b"lost=1", 0.4)  # must not commit without a majority
        with self.assertRaises(Exception):
            ld.read_barrier(0.3)  # must fail without a majority

    def test_partitioned_leader_loses_uncommitted_writes(self):
        self.new_cluster(3, 100)
        old = self.wait_leader()
        old_id = old.status().id
        self.propose("base=1")
        self.wait_applied(self.ids, "base", "1")

        self.net.isolate(old_id)

        def ghost():
            try:
                old.propose(b"ghost=1", 2.0)  # accepted by the isolated leader, can never commit
            except Exception:
                pass

        threading.Thread(target=ghost, daemon=True).start()
        rest = [i for i in self.ids if i != old_id]
        eventually(5, "new leader among the majority",
                   lambda: any(self.nodes[i].status().role == LEADER for i in rest))
        self.propose("real=1")
        self.net.heal(old_id)
        self.wait_applied(self.ids, "real", "1")
        eventually(5, "old leader to follow", lambda: self.nodes[old_id].status().role == FOLLOWER)
        for i in self.ids:
            self.assertIsNone(self.sms[i].get("ghost"), "%s applied an uncommitted write" % i)

    def test_follower_catches_up_from_log(self):
        self.new_cluster(3, 10000)
        ld = self.wait_leader()
        lagger = next(i for i in self.ids if i != ld.status().id)
        self.net.isolate(lagger)
        for i in range(100):
            self.propose("k%d=v" % i)
        self.net.heal(lagger)
        self.wait_applied([lagger], "k99", "v")

    def test_follower_catches_up_via_snapshot(self):
        self.new_cluster(3, 5)  # tiny log retention forces snapshots
        ld = self.wait_leader()
        lagger = next(i for i in self.ids if i != ld.status().id)
        self.net.isolate(lagger)
        for i in range(200):
            self.propose("k%03d=v%d" % (i, i))
            if i % 3 == 0:  # deletes must not be resurrected by the snapshot
                self.propose("-k%03d" % (i // 2))
        # Wait until the leader has really compacted its log past the lagger.
        lag_last = self.nodes[lagger].status().last_index
        eventually(5, "leader log compaction", lambda: self.leader().status().log_start > lag_last)
        # A write racing with the transfer exercises the fuzzy-snapshot repair.
        self.net.heal(lagger)
        for i in range(200, 260):
            self.propose("k%03d=v%d" % (i, i))
        self.wait_applied([lagger], "k259", "v259")

        def identical():
            ref = self.sms[self.leader().status().id].snapshot_data()
            return all(ref == self.sms[i].snapshot_data() for i in self.ids)

        eventually(8, "identical state on all nodes", identical)
        n = len(self.sms[lagger].history_copy())
        self.assertLessEqual(n, 150, "lagger applied %d commands individually; a snapshot should skip most" % n)

    def test_restart_keeps_state_and_term(self):
        self.new_cluster(3, 100)
        self.wait_leader()
        for i in range(15):
            self.propose("k%d=v" % i)
        self.wait_applied(self.ids, "k14", "v")
        # Restart the whole cluster, one node at a time and then all at once.
        for nid in self.ids:
            self.crash(nid)
            self.start_node(nid)
            self.wait_leader()
        for nid in self.ids:
            self.crash(nid)
        for nid in self.ids:
            self.start_node(nid)
        self.wait_leader()
        self.propose("post=1")
        self.wait_applied(self.ids, "post", "1")
        self.wait_applied(self.ids, "k0", "v")

    def test_read_barrier_on_healthy_leader(self):
        self.new_cluster(3, 100)
        ld = self.wait_leader()
        self.propose("x=1")
        ld.read_barrier(2.0)
        self.assertEqual(self.sms[ld.status().id].get("x"), "1")
        for nid in self.ids:
            if nid == ld.status().id:
                continue
            with self.assertRaises(NotLeaderError):
                self.nodes[nid].read_barrier(1.0)

    def test_partitioned_node_does_not_disrupt_leader(self):
        """A node that was cut off must not depose a healthy leader when it
        returns (pre-vote keeps its term from inflating)."""
        self.new_cluster(3, 100)
        ld = self.wait_leader()
        term = ld.status().term
        victim = next(i for i in self.ids if i != ld.status().id)
        self.net.isolate(victim)
        time.sleep(1.2)  # many election timeouts
        self.net.heal(victim)
        time.sleep(0.6)
        cur = self.wait_leader()
        self.assertEqual((cur.status().id, cur.status().term), (ld.status().id, term))

    def test_log_never_compacted_past_durable_index(self):
        """The log may only be compacted up to what the state machine reports
        durable: after a crash the state machine can be that far behind."""
        self.new_cluster(3, 5)
        self.wait_leader()
        for i in range(40):
            self.propose("a%d=1" % i)
        self.wait_applied(self.ids, "a39", "1")
        eventually(5, "initial compaction", lambda: self.leader().status().log_start > 20)

        limit = 60
        for sm in self.sms.values():
            sm.set_sync_limit(limit)
        for i in range(200):
            self.propose("b%d=1" % i)
        self.wait_applied(self.ids, "b199", "1")
        time.sleep(0.3)  # several sync ticks
        for nid, n in self.nodes.items():
            self.assertLessEqual(n.status().log_start, limit,
                                 "%s compacted its log past the durable index" % nid)
        for sm in self.sms.values():
            sm.set_sync_limit(0)
        eventually(5, "compaction to resume", lambda: self.leader().status().log_start > 150)


if __name__ == "__main__":
    unittest.main()
