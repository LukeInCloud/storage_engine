package replica

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"kvstore/engine"
	"kvstore/kvnet"
	"kvstore/raft"
)

type tcluster struct {
	t       *testing.T
	peers   []Peer
	dirs    map[string]string
	tune    func(*raft.Config)
	servers map[string]*Server

	mu   sync.Mutex
	logs []string
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func newTCluster(t *testing.T, size int, tune func(*raft.Config)) *tcluster {
	c := &tcluster{t: t, dirs: map[string]string{}, servers: map[string]*Server{}, tune: tune}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("n%d", i)
		c.peers = append(c.peers, Peer{ID: id, RaftAddr: freeAddr(t), ClientAddr: freeAddr(t)})
		c.dirs[id] = t.TempDir()
	}
	for _, p := range c.peers {
		c.start(p.ID)
	}
	t.Cleanup(func() {
		for _, s := range c.servers {
			s.Close()
		}
	})
	return c
}

func (c *tcluster) start(id string) {
	s, err := StartServer(NodeConfig{
		ID: id, Dir: c.dirs[id], Peers: c.peers,
		Engine: engine.Options{NoSync: true},
		Tune: func(rc *raft.Config) {
			rc.ElectionTimeout = 200 * time.Millisecond
			rc.HeartbeatInterval = 25 * time.Millisecond
			rc.NoSync = true
			if c.tune != nil {
				c.tune(rc)
			}
		},
		Logf: func(f string, a ...any) {
			c.mu.Lock()
			c.logs = append(c.logs, fmt.Sprintf(f, a...))
			c.mu.Unlock()
		},
	})
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	c.servers[id] = s
}

func (c *tcluster) stop(id string) {
	c.servers[id].Close()
	delete(c.servers, id)
}

func (c *tcluster) logged(substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, l := range c.logs {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

func (c *tcluster) clientAddrs() []string {
	var a []string
	for _, p := range c.peers {
		a = append(a, p.ClientAddr)
	}
	return a
}

func (c *tcluster) client() *kvnet.Client {
	cl, err := kvnet.DialCluster(c.clientAddrs(), 3*time.Second, 20*time.Second)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

func (c *tcluster) leaderID() string {
	for id, s := range c.servers {
		if s.Node.Status().Role == raft.Leader {
			return id
		}
	}
	return ""
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (c *tcluster) waitLeader() string {
	var id string
	waitFor(c.t, 10*time.Second, "a leader", func() bool { id = c.leaderID(); return id != "" })
	return id
}

func dump(t *testing.T, s *Server) []engine.KV {
	t.Helper()
	kvs, err := s.Machine.Range(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return kvs
}

// waitConverged waits until all given servers hold identical data equal to want.
func (c *tcluster) waitConverged(want map[string]string, ids ...string) {
	c.t.Helper()
	if len(ids) == 0 {
		for id := range c.servers {
			ids = append(ids, id)
		}
	}
	waitFor(c.t, 15*time.Second, "replicas to converge", func() bool {
		for _, id := range ids {
			kvs, err := c.servers[id].Machine.Range(nil, nil)
			if err != nil || len(kvs) != len(want) {
				return false
			}
			for _, kv := range kvs {
				if want[string(kv.Key)] != string(kv.Value) {
					return false
				}
			}
		}
		return true
	})
}

func TestClusterAllOperations(t *testing.T) {
	c := newTCluster(t, 3, nil)
	c.waitLeader()
	cl := c.client()

	if _, err := cl.Read([]byte("nope")); err != engine.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if err := cl.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := cl.BatchPut([][]byte{[]byte("b"), []byte("c"), []byte("d")}, [][]byte{[]byte("2"), []byte("3"), []byte("4")}); err != nil {
		t.Fatal(err)
	}
	if err := cl.Delete([]byte("c")); err != nil {
		t.Fatal(err)
	}
	if v, err := cl.Read([]byte("b")); err != nil || string(v) != "2" {
		t.Fatalf("Read b = %q %v", v, err)
	}
	kvs, err := cl.ReadKeyRange([]byte("a"), []byte("d"))
	if err != nil || len(kvs) != 3 {
		t.Fatalf("range = %v %v", kvs, err)
	}
	// Every replica holds the data; internal bookkeeping keys stay hidden.
	c.waitConverged(map[string]string{"a": "1", "b": "2", "d": "4"})

	if err := cl.Put([]byte("\x00raft/applied"), []byte("x")); err == nil {
		t.Fatal("reserved key must be rejected")
	}
	if err := cl.Put(nil, []byte("x")); err == nil {
		t.Fatal("empty key must be rejected")
	}
}

func TestClusterLeaderFailoverAndRejoin(t *testing.T) {
	c := newTCluster(t, 3, nil)
	c.waitLeader()
	cl := c.client()
	want := map[string]string{}

	put := func(i int) {
		k, v := fmt.Sprintf("key-%04d", i), fmt.Sprintf("val-%d", i)
		if err := cl.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		want[k] = v
	}
	for i := 0; i < 50; i++ {
		put(i)
	}

	dead := c.leaderID()
	c.stop(dead)
	// The same client keeps working: it fails over to the new leader by itself.
	for i := 50; i < 100; i++ {
		put(i)
	}
	if v, err := cl.Read([]byte("key-0007")); err != nil || string(v) != "val-7" {
		t.Fatalf("read after failover: %q %v", v, err)
	}
	if nl := c.leaderID(); nl == "" || nl == dead {
		t.Fatalf("bad leader after failover: %q", nl)
	}
	var live []string
	for id := range c.servers {
		live = append(live, id)
	}
	c.waitConverged(want, live...)

	// The old leader comes back and catches up from its own disk + the log.
	c.start(dead)
	c.waitConverged(want)
}

func TestClusterWholeRestart(t *testing.T) {
	c := newTCluster(t, 3, nil)
	c.waitLeader()
	cl := c.client()
	want := map[string]string{}
	for i := 0; i < 40; i++ {
		k, v := fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i)
		if err := cl.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	cl.Delete([]byte("k005"))
	delete(want, "k005")
	c.waitConverged(want)

	for _, p := range c.peers {
		c.stop(p.ID)
	}
	for _, p := range c.peers {
		c.start(p.ID)
	}
	c.waitLeader()
	c.waitConverged(want)
	cl2 := c.client()
	if v, err := cl2.Read([]byte("k039")); err != nil || string(v) != "v39" {
		t.Fatalf("after restart: %q %v", v, err)
	}
	if err := cl2.Put([]byte("new"), []byte("1")); err != nil {
		t.Fatal(err)
	}
}

func TestClusterSnapshotCatchUpWithRealEngine(t *testing.T) {
	c := newTCluster(t, 3, func(rc *raft.Config) {
		rc.LogRetain = 20
		rc.SnapshotChunkBytes = 4 << 10
	})
	c.waitLeader()
	cl := c.client()

	lagger := ""
	for id := range c.servers {
		if id != c.leaderID() {
			lagger = id
			break
		}
	}
	c.stop(lagger)

	want := map[string]string{}
	val := strings.Repeat("x", 200)
	for i := 0; i < 400; i++ {
		k, v := fmt.Sprintf("key-%04d", i), fmt.Sprintf("%d-%s", i, val)
		if err := cl.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
		if i%7 == 0 {
			dk := fmt.Sprintf("key-%04d", i/2)
			if err := cl.Delete([]byte(dk)); err != nil {
				t.Fatal(err)
			}
			delete(want, dk)
		}
	}
	c.start(lagger)
	// Writes during the transfer exercise the fuzzy-snapshot repair.
	for i := 400; i < 460; i++ {
		k, v := fmt.Sprintf("key-%04d", i), fmt.Sprintf("late-%d", i)
		if err := cl.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	c.waitConverged(want)
	if !c.logged("installed snapshot") {
		t.Fatal("the lagging node should have been caught up by a snapshot")
	}
	// It is a normal, writable member afterwards.
	if err := cl.Put([]byte("final"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	want["final"] = "1"
	c.waitConverged(want)
}

// A single client doing read-your-writes must never observe stale data, not
// even while the leader is killed under it.
func TestClusterLinearizableUnderFailover(t *testing.T) {
	c := newTCluster(t, 3, nil)
	c.waitLeader()
	cl := c.client()
	killed := false
	for i := 1; i <= 120; i++ {
		v := []byte(fmt.Sprintf("%d", i))
		if err := cl.Put([]byte("counter"), v); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		got, err := cl.Read([]byte("counter"))
		if err != nil || !bytes.Equal(got, v) {
			t.Fatalf("after put %d read %q (%v)", i, got, err)
		}
		if i == 60 && !killed {
			c.stop(c.leaderID())
			killed = true
		}
	}
}

func TestClusterConcurrentClients(t *testing.T) {
	c := newTCluster(t, 3, nil)
	c.waitLeader()
	var wg sync.WaitGroup
	var mu sync.Mutex
	want := map[string]string{}
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cl := c.client()
			for i := 0; i < 60; i++ {
				k, v := fmt.Sprintf("w%d-%03d", w, i), fmt.Sprintf("v%d", i)
				if err := cl.Put([]byte(k), []byte(v)); err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				want[k] = v
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	c.waitConverged(want)
	a, b := dump(t, c.servers["n1"]), dump(t, c.servers["n2"])
	if !reflect.DeepEqual(a, b) {
		t.Fatal("replicas differ")
	}
}
