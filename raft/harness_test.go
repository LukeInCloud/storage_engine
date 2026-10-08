package raft

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- in-memory transport with partitions ----

type memNet struct {
	mu      sync.Mutex
	nodes   map[string]*Node
	blocked map[string]bool // node isolated from everyone
}

func newMemNet() *memNet {
	return &memNet{nodes: map[string]*Node{}, blocked: map[string]bool{}}
}

func (m *memNet) set(id string, n *Node) { m.mu.Lock(); m.nodes[id] = n; m.mu.Unlock() }
func (m *memNet) isolate(id string)      { m.mu.Lock(); m.blocked[id] = true; m.mu.Unlock() }
func (m *memNet) heal(id string)         { m.mu.Lock(); delete(m.blocked, id); m.mu.Unlock() }

func (m *memNet) route(from, to string) (*Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blocked[from] || m.blocked[to] {
		return nil, errors.New("partitioned")
	}
	n := m.nodes[to]
	if n == nil {
		return nil, errors.New("node down")
	}
	return n, nil
}

type memTransport struct {
	net  *memNet
	self string
}

func (t *memTransport) RequestVote(to string, r *VoteReq) (*VoteResp, error) {
	n, err := t.net.route(t.self, to)
	if err != nil {
		return nil, err
	}
	return n.HandleRequestVote(r)
}
func (t *memTransport) AppendEntries(to string, r *AppendReq) (*AppendResp, error) {
	n, err := t.net.route(t.self, to)
	if err != nil {
		return nil, err
	}
	return n.HandleAppendEntries(r)
}
func (t *memTransport) InstallSnapshot(to string, r *SnapReq) (*SnapResp, error) {
	n, err := t.net.route(t.self, to)
	if err != nil {
		return nil, err
	}
	return n.HandleInstallSnapshot(r)
}

// ---- toy state machine: commands are "k=v" (set) or "-k" (delete) ----

type memSM struct {
	mu      sync.Mutex
	data    map[string]string
	index   uint64
	term    uint64
	history []string // every applied command, in order
	staging map[string]string
}

func newMemSM() *memSM { return &memSM{data: map[string]string{}} }

func (s *memSM) Applied() (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.index, s.term, nil
}

func (s *memSM) Apply(index, term uint64, cmds [][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range cmds {
		cmd := string(c)
		s.history = append(s.history, cmd)
		if strings.HasPrefix(cmd, "-") {
			delete(s.data, cmd[1:])
		} else if k, v, ok := strings.Cut(cmd, "="); ok {
			s.data[k] = v
		}
	}
	s.index, s.term = index, term
	return nil
}

func (s *memSM) SnapshotScan(cursor []byte, maxBytes int) ([]KV, []byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for k := range s.data {
		if k >= string(cursor) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var out []KV
	size := 0
	for i, k := range keys {
		if len(out) > 0 && size >= maxBytes {
			return out, []byte(keys[i]), false, nil
		}
		out = append(out, KV{[]byte(k), []byte(s.data[k])})
		size += len(k) + len(s.data[k])
	}
	return out, nil, true, nil
}

func (s *memSM) RestoreBegin() error {
	s.mu.Lock()
	s.staging = map[string]string{}
	s.mu.Unlock()
	return nil
}
func (s *memSM) RestoreChunk(items []KV) error {
	s.mu.Lock()
	for _, kv := range items {
		s.staging[string(kv.Key)] = string(kv.Value)
	}
	s.mu.Unlock()
	return nil
}
func (s *memSM) RestoreFinish(index, term uint64) error {
	s.mu.Lock()
	s.data, s.staging, s.index, s.term = s.staging, nil, index, term
	s.mu.Unlock()
	return nil
}
func (s *memSM) RestoreAbort() { s.mu.Lock(); s.staging = nil; s.mu.Unlock() }

func (s *memSM) get(k string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[k]
	return v, ok
}

func (s *memSM) snapshotData() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := map[string]string{}
	for k, v := range s.data {
		c[k] = v
	}
	return c
}

// A durable toy SM: survives node restarts like the real engine would.
// (memSM instances are simply reused across a restart of the Node.)

// ---- cluster ----

type cluster struct {
	t      *testing.T
	net    *memNet
	ids    []string
	dirs   map[string]string
	nodes  map[string]*Node
	sms    map[string]*memSM
	retain int
}

func newCluster(t *testing.T, size int, retain int) *cluster {
	c := &cluster{t: t, net: newMemNet(), dirs: map[string]string{}, nodes: map[string]*Node{}, sms: map[string]*memSM{}, retain: retain}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.dirs[id] = t.TempDir()
		c.sms[id] = newMemSM()
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *cluster) start(id string) {
	n, err := NewNode(Config{
		ID: id, Peers: c.ids, Dir: c.dirs[id],
		Transport:          &memTransport{c.net, id},
		SM:                 c.sms[id],
		ElectionTimeout:    150 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
		LogRetain:          c.retain,
		SnapshotChunkBytes: 64,
		NoSync:             true,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = n
	c.net.set(id, n)
	n.Start()
}

// crash stops a node without touching its disk or state machine.
func (c *cluster) crash(id string) {
	c.nodes[id].Stop()
	c.net.mu.Lock()
	delete(c.net.nodes, id)
	c.net.mu.Unlock()
	delete(c.nodes, id)
}

func (c *cluster) leader() *Node {
	var ld *Node
	for _, n := range c.nodes {
		st := n.Status()
		if st.Role == Leader {
			c.net.mu.Lock()
			isolated := c.net.blocked[st.ID]
			c.net.mu.Unlock()
			if isolated {
				continue
			}
			if ld != nil && ld.Status().Term == st.Term {
				c.t.Fatalf("two leaders in term %d", st.Term)
			}
			if ld == nil || st.Term > ld.Status().Term {
				ld = n
			}
		}
	}
	return ld
}

func (c *cluster) waitLeader() *Node {
	c.t.Helper()
	var ld *Node
	eventually(c.t, 5*time.Second, "a leader", func() bool { ld = c.leader(); return ld != nil })
	return ld
}

func (c *cluster) propose(cmd string) error {
	var last error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ld := c.leader()
		if ld == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if last = ld.Propose([]byte(cmd), 2*time.Second); last == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("propose %q: %v", cmd, last)
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (c *cluster) waitApplied(ids []string, key, val string) {
	c.t.Helper()
	eventually(c.t, 8*time.Second, fmt.Sprintf("%v to apply %s=%s", ids, key, val), func() bool {
		for _, id := range ids {
			if v, ok := c.sms[id].get(key); !ok || v != val {
				return false
			}
		}
		return true
	})
}
