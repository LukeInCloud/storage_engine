package replica

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kvstore/raft"
)

// Writers hammer the cluster while nodes are repeatedly stopped and restarted
// (one at a time, so a majority always exists). Every acknowledged write must
// be present on every replica at the end, and replicas must be identical.
func TestChaosKillRestartNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newTCluster(t, 3, func(rc *raft.Config) {
		rc.LogRetain = 200 // also exercise snapshots when a node returns
		rc.SnapshotChunkBytes = 8 << 10
	})
	c.waitLeader()

	var mu sync.Mutex
	want := map[string]string{}
	var failures atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cl := c.client()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k, v := fmt.Sprintf("w%d-%06d", w, i), fmt.Sprintf("value-%d-%d", w, i)
				if err := cl.Put([]byte(k), []byte(v)); err != nil {
					failures.Add(1)
					t.Errorf("put %s failed after retries: %v", k, err)
					return
				}
				mu.Lock()
				want[k] = v
				mu.Unlock()
				if i%9 == 0 { // reads must be consistent with acknowledged writes
					got, err := cl.Read([]byte(k))
					if err != nil || string(got) != v {
						t.Errorf("read-your-write %s = %q, %v", k, got, err)
						return
					}
				}
			}
		}(w)
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	end := time.Now().Add(12 * time.Second)
	kills := 0
	for time.Now().Before(end) && failures.Load() == 0 {
		victim := c.peers[rnd.Intn(len(c.peers))].ID
		c.stop(victim)
		kills++
		time.Sleep(time.Duration(200+rnd.Intn(700)) * time.Millisecond)
		c.start(victim)
		time.Sleep(time.Duration(300+rnd.Intn(500)) * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if t.Failed() {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	t.Logf("%d node kills, %d acknowledged writes", kills, len(want))
	if len(want) < 100 {
		t.Fatalf("too few writes (%d) to be meaningful", len(want))
	}
	c.waitConverged(want)
}
