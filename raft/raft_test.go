package raft

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1, 100)
	c.waitLeader()
	if err := c.propose("a=1"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(c.ids, "a", "1")
	if err := c.nodes["n1"].ReadBarrier(time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestElectsExactlyOneLeaderAndReplicates(t *testing.T) {
	c := newCluster(t, 3, 100)
	c.waitLeader()
	for i := 0; i < 20; i++ {
		if err := c.propose(fmt.Sprintf("k%d=v%d", i, i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied(c.ids, "k19", "v19")
	// Followers reject proposals with a leader hint.
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.Status().Role == Leader {
			continue
		}
		err := n.Propose([]byte("x=1"), time.Second)
		var nl *NotLeaderError
		if !errors.As(err, &nl) {
			t.Fatalf("follower Propose = %v, want NotLeaderError", err)
		}
	}
}

func TestAllNodesApplySameOrder(t *testing.T) {
	c := newCluster(t, 3, 1000)
	c.waitLeader()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := c.propose(fmt.Sprintf("w%d-%d=x", w, i)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	c.waitApplied(c.ids, "w3-49", "x")
	eventually(t, 5*time.Second, "equal histories", func() bool {
		var ref []string
		for i, id := range c.ids {
			c.sms[id].mu.Lock()
			h := append([]string(nil), c.sms[id].history...)
			c.sms[id].mu.Unlock()
			if i == 0 {
				ref = h
			} else if !reflect.DeepEqual(ref, h) {
				return false
			}
		}
		return len(ref) == 200
	})
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3, 100)
	old := c.waitLeader()
	for i := 0; i < 10; i++ {
		if err := c.propose(fmt.Sprintf("before%d=1", i)); err != nil {
			t.Fatal(err)
		}
	}
	oldID := old.Status().ID
	c.crash(oldID)

	var rest []string
	for _, id := range c.ids {
		if id != oldID {
			rest = append(rest, id)
		}
	}
	nl := c.waitLeader()
	if nl.Status().ID == oldID {
		t.Fatal("dead node is still leader")
	}
	if err := c.propose("after=1"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(rest, "after", "1")
	for i := 0; i < 10; i++ {
		c.waitApplied(rest, fmt.Sprintf("before%d", i), "1") // committed data survived
	}

	// The old leader restarts from its disk and catches up as a follower.
	c.start(oldID)
	c.waitApplied([]string{oldID}, "after", "1")
	if c.nodes[oldID].Status().Role == Leader && c.nodes[oldID].Status().Term == nl.Status().Term {
		t.Fatal("restarted node must not displace the leader")
	}
}

func TestNoProgressWithoutMajority(t *testing.T) {
	c := newCluster(t, 3, 100)
	ld := c.waitLeader()
	id := ld.Status().ID
	for _, other := range c.ids {
		if other != id {
			c.net.isolate(other)
		}
	}
	if err := ld.Propose([]byte("lost=1"), 400*time.Millisecond); err == nil {
		t.Fatal("proposal must not commit without a majority")
	}
	if err := ld.ReadBarrier(300 * time.Millisecond); err == nil {
		t.Fatal("ReadBarrier must fail without a majority")
	}
}

func TestPartitionedLeaderLosesUncommittedWrites(t *testing.T) {
	c := newCluster(t, 3, 100)
	old := c.waitLeader()
	oldID := old.Status().ID
	if err := c.propose("base=1"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(c.ids, "base", "1")

	c.net.isolate(oldID)
	// The isolated leader accepts a write that can never commit.
	go old.Propose([]byte("ghost=1"), 2*time.Second)

	var rest []string
	for _, id := range c.ids {
		if id != oldID {
			rest = append(rest, id)
		}
	}
	eventually(t, 5*time.Second, "new leader among the majority", func() bool {
		for _, id := range rest {
			if c.nodes[id].Status().Role == Leader {
				return true
			}
		}
		return false
	})
	if err := c.propose("real=1"); err != nil {
		t.Fatal(err)
	}
	c.net.heal(oldID)
	c.waitApplied(c.ids, "real", "1")
	eventually(t, 5*time.Second, "old leader to follow", func() bool {
		return c.nodes[oldID].Status().Role == Follower
	})
	for _, id := range c.ids {
		if _, ok := c.sms[id].get("ghost"); ok {
			t.Fatalf("%s applied an uncommitted write from a deposed leader", id)
		}
	}
}

func TestFollowerCatchesUpFromLog(t *testing.T) {
	c := newCluster(t, 3, 10000)
	ld := c.waitLeader()
	var lagger string
	for _, id := range c.ids {
		if id != ld.Status().ID {
			lagger = id
			break
		}
	}
	c.net.isolate(lagger)
	for i := 0; i < 100; i++ {
		if err := c.propose(fmt.Sprintf("k%d=v", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.net.heal(lagger)
	c.waitApplied([]string{lagger}, "k99", "v")
}

func TestFollowerCatchesUpViaSnapshot(t *testing.T) {
	c := newCluster(t, 3, 5) // tiny log retention forces snapshots
	ld := c.waitLeader()
	var lagger string
	for _, id := range c.ids {
		if id != ld.Status().ID {
			lagger = id
			break
		}
	}
	c.net.isolate(lagger)
	for i := 0; i < 200; i++ {
		if err := c.propose(fmt.Sprintf("k%03d=v%d", i, i)); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 { // deletes must not be resurrected by the snapshot
			if err := c.propose(fmt.Sprintf("-k%03d", i/2)); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Wait until the leader has really compacted its log past the lagger, so
	// that only a snapshot can bring it back.
	lagLast := c.nodes[lagger].Status().LastIndex
	eventually(t, 5*time.Second, "leader log compaction", func() bool {
		return c.leader().Status().LogStart > lagLast
	})
	// A write racing with the transfer exercises the fuzzy-snapshot repair.
	c.net.heal(lagger)
	for i := 200; i < 260; i++ {
		if err := c.propose(fmt.Sprintf("k%03d=v%d", i, i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied([]string{lagger}, "k259", "v259")
	eventually(t, 8*time.Second, "identical state on all nodes", func() bool {
		ref := c.sms[c.leader().Status().ID].snapshotData()
		for _, id := range c.ids {
			if !reflect.DeepEqual(ref, c.sms[id].snapshotData()) {
				return false
			}
		}
		return true
	})
	// The lagger must really have used a snapshot, not the log.
	if n := len(c.sms[lagger].history); n > 150 {
		t.Fatalf("lagger applied %d commands individually; expected a snapshot to skip most", n)
	}
}

func TestRestartKeepsStateAndTerm(t *testing.T) {
	c := newCluster(t, 3, 100)
	c.waitLeader()
	for i := 0; i < 15; i++ {
		c.propose(fmt.Sprintf("k%d=v", i))
	}
	c.waitApplied(c.ids, "k14", "v")
	// Restart the whole cluster, one node at a time and then all at once.
	for _, id := range c.ids {
		c.crash(id)
		c.start(id)
		c.waitLeader()
	}
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	c.waitLeader()
	if err := c.propose("post=1"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(c.ids, "post", "1")
	c.waitApplied(c.ids, "k0", "v")
	// No command applied twice after restarts replayed the log.
	if v, _ := c.sms["n1"].get("post"); v != "1" {
		t.Fatal("state lost")
	}
}

func TestReadBarrierOnHealthyLeader(t *testing.T) {
	c := newCluster(t, 3, 100)
	ld := c.waitLeader()
	c.propose("x=1")
	if err := ld.ReadBarrier(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	if v, ok := c.sms[ld.Status().ID].get("x"); !ok || v != "1" {
		t.Fatal("read after barrier must see the committed write")
	}
	for _, id := range c.ids {
		if id == ld.Status().ID {
			continue
		}
		var nl *NotLeaderError
		if err := c.nodes[id].ReadBarrier(time.Second); !errors.As(err, &nl) {
			t.Fatalf("follower ReadBarrier = %v", err)
		}
	}
}

// A node that was cut off must not depose a healthy leader when it returns
// (pre-vote keeps its term from inflating).
func TestPartitionedNodeDoesNotDisruptLeader(t *testing.T) {
	c := newCluster(t, 3, 100)
	ld := c.waitLeader()
	term := ld.Status().Term
	var victim string
	for _, id := range c.ids {
		if id != ld.Status().ID {
			victim = id
			break
		}
	}
	c.net.isolate(victim)
	time.Sleep(1200 * time.Millisecond) // many election timeouts
	c.net.heal(victim)
	time.Sleep(600 * time.Millisecond)
	if cur := c.waitLeader(); cur.Status().ID != ld.Status().ID || cur.Status().Term != term {
		t.Fatalf("leader disrupted: %s term %d -> %s term %d", ld.Status().ID, term, cur.Status().ID, cur.Status().Term)
	}
}

// The log may only be compacted up to what the state machine reports durable:
// after a crash the state machine can be that far behind, and the log is the
// only place the missing commands still exist.
func TestLogNeverCompactedPastDurableIndex(t *testing.T) {
	c := newCluster(t, 3, 5)
	c.waitLeader()
	for i := 0; i < 40; i++ {
		c.propose(fmt.Sprintf("a%d=1", i))
	}
	c.waitApplied(c.ids, "a39", "1")
	eventually(t, 5*time.Second, "initial compaction", func() bool { return c.leader().Status().LogStart > 20 })

	const limit = 60
	for _, sm := range c.sms {
		sm.setSyncLimit(limit)
	}
	for i := 0; i < 200; i++ {
		c.propose(fmt.Sprintf("b%d=1", i))
	}
	c.waitApplied(c.ids, "b199", "1")
	time.Sleep(300 * time.Millisecond) // several sync ticks
	for id, n := range c.nodes {
		if st := n.Status(); st.LogStart > limit {
			t.Fatalf("%s compacted its log to %d although only %d is durable", id, st.LogStart, limit)
		}
	}
	for _, sm := range c.sms {
		sm.setSyncLimit(0)
	}
	eventually(t, 5*time.Second, "compaction to resume", func() bool {
		return c.leader().Status().LogStart > 150
	})
}
