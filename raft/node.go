package raft

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string { return [...]string{"follower", "candidate", "leader"}[r] }

type Config struct {
	ID    string
	Peers []string // IDs of ALL nodes in the cluster, including this one
	Dir   string   // holds raft.log and state; created if missing

	Transport Transport
	SM        StateMachine

	// ElectionTimeout is the minimum; the effective timeout is random in [T, 2T).
	ElectionTimeout   time.Duration // default 400ms
	HeartbeatInterval time.Duration // default ElectionTimeout/8

	LogRetain          int // applied entries kept in the log for slow followers (default 8192)
	MaxBatchEntries    int // entries per AppendEntries (default 512)
	MaxBatchBytes      int // bytes per AppendEntries (default 4 MiB)
	MaxCommandBytes    int // largest accepted command (default 32 MiB)
	SnapshotChunkBytes int // default 1 MiB

	NoSync bool // skip fsync of the Raft log (tests only)
	Logf   func(format string, args ...any)
}

func (c *Config) setDefaults() {
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = 400 * time.Millisecond
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = c.ElectionTimeout / 8
	}
	if c.LogRetain <= 0 {
		c.LogRetain = 8192
	}
	if c.MaxBatchEntries <= 0 {
		c.MaxBatchEntries = 512
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = 4 << 20
	}
	if c.MaxCommandBytes <= 0 {
		c.MaxCommandBytes = 32 << 20
	}
	if c.SnapshotChunkBytes <= 0 {
		c.SnapshotChunkBytes = 1 << 20
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
}

type proposal struct {
	data  []byte
	term  uint64
	index uint64
	done  chan error // buffered(1)
}

type restoreSession struct {
	id       uint64
	nextSeq  uint64
	snapIdx  uint64
	snapTerm uint64
	started  bool
}

type Node struct {
	cfg    Config
	others []string

	mu   sync.Mutex
	cond *sync.Cond // broadcast on any state change waiters care about

	stopCh  chan struct{}
	wg      sync.WaitGroup
	stopped bool
	failed  error

	role     Role
	term     uint64
	votedFor string
	leader   string

	lastContact      time.Time // last valid message from a leader
	electionDeadline time.Time

	log       *logStore
	commit    uint64
	applied   uint64 // applied in memory (includes trailing no-ops)
	smApplied uint64 // last index handed to the state machine (durable)
	applying  bool
	restoring *restoreSession

	// leader state
	next    map[string]uint64
	match   map[string]uint64
	ackTime map[string]time.Time // send time of the newest request each peer acknowledged
	snapPin map[string]uint64    // log must be kept from this index while a snapshot streams
	waiters map[uint64]*proposal
	kick    map[string]chan struct{}

	propMu  sync.Mutex
	propQ   []*proposal
	propSig chan struct{}

	rnd *rand.Rand
}

func NewNode(cfg Config) (*Node, error) {
	cfg.setDefaults()
	if cfg.Transport == nil || cfg.SM == nil || cfg.ID == "" {
		return nil, errors.New("raft: ID, Transport and SM are required")
	}
	found := false
	n := &Node{cfg: cfg}
	for _, p := range cfg.Peers {
		if p == cfg.ID {
			found = true
		} else {
			n.others = append(n.others, p)
		}
	}
	if !found {
		return nil, errors.New("raft: Peers must include this node's ID")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	var err error
	if n.term, n.votedFor, err = loadHardState(cfg.Dir); err != nil {
		return nil, err
	}
	if n.log, err = openLog(cfg.Dir, cfg.NoSync); err != nil {
		return nil, err
	}
	ai, at, err := cfg.SM.Applied()
	if err != nil {
		n.log.close()
		return nil, err
	}
	switch {
	case ai > n.log.lastIndex():
		// The state machine is ahead of the log (a snapshot restore finished but
		// the log reset did not): the log is obsolete.
		if err := n.log.reset(ai, at); err != nil {
			n.log.close()
			return nil, err
		}
	case ai < n.log.prevIndex:
		n.log.close()
		return nil, fmt.Errorf("raft: state machine (applied %d) is behind the log start (%d)", ai, n.log.prevIndex)
	}
	n.commit, n.applied, n.smApplied = ai, ai, ai

	n.cond = sync.NewCond(&n.mu)
	n.stopCh = make(chan struct{})
	n.propSig = make(chan struct{}, 1)
	n.kick = map[string]chan struct{}{}
	for _, p := range n.others {
		n.kick[p] = make(chan struct{}, 1)
	}
	h := fnv.New64a()
	h.Write([]byte(cfg.ID))
	n.rnd = rand.New(rand.NewSource(int64(h.Sum64()) ^ time.Now().UnixNano()))
	n.resetElectionTimer()
	return n, nil
}

func (n *Node) Start() {
	n.wg.Add(3)
	go n.electionLoop()
	go n.applyLoop()
	go n.appendLoop()
	for _, p := range n.others {
		n.wg.Add(1)
		go n.replicator(p)
	}
}

func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.cond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
	n.mu.Lock()
	n.failWaiters(ErrStopped)
	n.mu.Unlock()
	n.failQueued(ErrStopped)
	n.log.close()
}

type Status struct {
	ID        string
	Role      Role
	Term      uint64
	Leader    string
	Commit    uint64
	Applied   uint64
	LastIndex uint64
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{n.cfg.ID, n.role, n.term, n.leader, n.commit, n.applied, n.log.lastIndex()}
}

// ---- helpers (mu held unless noted) ----

func (n *Node) quorum() int { return (len(n.others)+1)/2 + 1 }

func (n *Node) resetElectionTimer() {
	t := n.cfg.ElectionTimeout
	n.electionDeadline = time.Now().Add(t + time.Duration(n.rnd.Int63n(int64(t))))
}

func (n *Node) fatal(err error) {
	if n.failed == nil {
		n.failed = err
		n.cfg.Logf("[%s] FATAL: %v", n.cfg.ID, err)
		log.Printf("raft[%s]: fatal: %v", n.cfg.ID, err)
	}
	n.role = Follower
	n.failWaiters(err)
	n.cond.Broadcast()
}

func (n *Node) persistHardState() {
	if err := saveHardState(n.cfg.Dir, n.term, n.votedFor); err != nil {
		n.fatal(err)
	}
}

func (n *Node) failWaiters(err error) {
	for idx, p := range n.waiters {
		p.done <- err
		delete(n.waiters, idx)
	}
}

func (n *Node) notLeader() error { return &NotLeaderError{Leader: n.leader} }

// stepDown moves to follower in term (>= current term).
func (n *Node) stepDown(term uint64) {
	if term > n.term {
		n.term, n.votedFor, n.leader = term, "", ""
		n.persistHardState()
	}
	if n.role == Leader {
		n.cfg.Logf("[%s] stepping down in term %d", n.cfg.ID, n.term)
		n.failWaiters(n.notLeader())
	}
	n.role = Follower
	n.resetElectionTimer()
	n.cond.Broadcast()
}

// waitUntil blocks (releasing mu) until pred() or the deadline/stop. mu held.
func (n *Node) waitUntil(deadline time.Time, pred func() bool) error {
	t := time.AfterFunc(time.Until(deadline), func() {
		n.mu.Lock()
		n.cond.Broadcast()
		n.mu.Unlock()
	})
	defer t.Stop()
	for !pred() {
		if n.stopped {
			return ErrStopped
		}
		if n.failed != nil {
			return n.failed
		}
		if !time.Now().Before(deadline) {
			return ErrTimeout
		}
		n.cond.Wait()
	}
	return nil
}

// ---- elections ----

func (n *Node) electionLoop() {
	defer n.wg.Done()
	tick := time.NewTicker(n.cfg.ElectionTimeout / 10)
	defer tick.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-tick.C:
		}
		n.mu.Lock()
		if n.failed == nil && n.role != Leader && time.Now().After(n.electionDeadline) {
			n.resetElectionTimer()
			term := n.term + 1
			li, lt := n.log.lastIndex(), n.log.lastTerm()
			n.wg.Add(1)
			go n.runElection(term, li, lt)
		}
		n.mu.Unlock()
	}
}

// gather asks all peers for votes and reports whether a majority granted.
func (n *Node) gather(pre bool, term, lastIdx, lastTerm uint64) bool {
	type result struct {
		resp *VoteResp
		err  error
	}
	ch := make(chan result, len(n.others))
	for _, p := range n.others {
		go func(p string) {
			resp, err := n.cfg.Transport.RequestVote(p, &VoteReq{
				Term: term, Candidate: n.cfg.ID, LastLogIndex: lastIdx, LastLogTerm: lastTerm, PreVote: pre,
			})
			ch <- result{resp, err}
		}(p)
	}
	votes := 1
	for range n.others {
		select {
		case r := <-ch:
			if r.err != nil || r.resp == nil {
				continue
			}
			if r.resp.Term > term {
				n.mu.Lock()
				if r.resp.Term > n.term {
					n.stepDown(r.resp.Term)
				}
				n.mu.Unlock()
				return false
			}
			if r.resp.Granted {
				votes++
			}
			if votes >= n.quorum() {
				return true
			}
		case <-n.stopCh:
			return false
		}
	}
	return votes >= n.quorum()
}

func (n *Node) runElection(term, lastIdx, lastTerm uint64) {
	defer n.wg.Done()
	if len(n.others) > 0 && !n.gather(true, term, lastIdx, lastTerm) {
		return
	}
	n.mu.Lock()
	if n.failed != nil || n.role == Leader || n.term+1 != term {
		n.mu.Unlock()
		return
	}
	n.term, n.votedFor, n.role, n.leader = term, n.cfg.ID, Candidate, ""
	n.persistHardState()
	n.resetElectionTimer()
	lastIdx, lastTerm = n.log.lastIndex(), n.log.lastTerm()
	n.mu.Unlock()

	won := len(n.others) == 0 || n.gather(false, term, lastIdx, lastTerm)
	n.mu.Lock()
	defer n.mu.Unlock()
	if won && n.term == term && n.role == Candidate && n.failed == nil {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.cfg.Logf("[%s] became leader in term %d", n.cfg.ID, n.term)
	n.role, n.leader = Leader, n.cfg.ID
	n.next, n.match = map[string]uint64{}, map[string]uint64{}
	n.ackTime, n.snapPin = map[string]time.Time{}, map[string]uint64{}
	n.waiters = map[uint64]*proposal{}
	for _, p := range n.others {
		n.next[p] = n.log.lastIndex() + 1
	}
	// A no-op in the new term lets entries of earlier terms commit and
	// lets ReadIndex know the leader's commit index is current.
	noop := Entry{Term: n.term, Index: n.log.lastIndex() + 1}
	if err := n.log.append([]Entry{noop}); err != nil {
		n.fatal(err)
		return
	}
	n.advanceCommit()
	n.kickAll()
	n.cond.Broadcast()
}

func (n *Node) kickAll() {
	for _, c := range n.kick {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

func (n *Node) HandleRequestVote(req *VoteReq) (*VoteResp, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if n.failed != nil {
		return nil, n.failed
	}
	upToDate := req.LastLogTerm > n.log.lastTerm() ||
		(req.LastLogTerm == n.log.lastTerm() && req.LastLogIndex >= n.log.lastIndex())
	if req.PreVote {
		// Would we vote for this candidate? Never disturb a live leader.
		hearing := n.role == Leader || time.Since(n.lastContact) < n.cfg.ElectionTimeout
		return &VoteResp{Term: n.term, Granted: req.Term >= n.term && upToDate && !hearing}, nil
	}
	if req.Term < n.term {
		return &VoteResp{Term: n.term}, nil
	}
	if req.Term > n.term {
		n.stepDown(req.Term)
	}
	resp := &VoteResp{Term: n.term}
	if (n.votedFor == "" || n.votedFor == req.Candidate) && upToDate {
		n.votedFor = req.Candidate
		n.persistHardState()
		if n.failed != nil {
			return nil, n.failed
		}
		n.resetElectionTimer()
		resp.Granted = true
	}
	return resp, nil
}

// ---- log replication: follower side ----

func (n *Node) HandleAppendEntries(req *AppendReq) (*AppendResp, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if n.failed != nil {
		return nil, n.failed
	}
	if req.Term < n.term {
		return &AppendResp{Term: n.term}, nil
	}
	if req.Term > n.term || n.role != Follower {
		n.stepDown(req.Term)
	}
	n.leader, n.lastContact = req.Leader, time.Now()
	n.resetElectionTimer()
	n.abortRestore()
	resp := &AppendResp{Term: n.term}
	l := n.log

	if req.PrevIndex > l.lastIndex() {
		resp.ConflictIndex = l.lastIndex() + 1
		return resp, nil
	}
	if req.PrevIndex >= l.prevIndex { // below that, everything is committed and matches
		if t, _ := l.term(req.PrevIndex); t != req.PrevTerm {
			resp.ConflictTerm = t
			resp.ConflictIndex = l.firstIndexOfTerm(t)
			if resp.ConflictIndex == 0 {
				resp.ConflictIndex = req.PrevIndex
			}
			return resp, nil
		}
	}
	var toAppend []Entry
	for i, e := range req.Entries {
		if e.Index <= l.prevIndex {
			continue
		}
		if e.Index <= l.lastIndex() {
			if t, _ := l.term(e.Index); t == e.Term {
				continue
			}
			if e.Index <= n.commit {
				n.fatal(fmt.Errorf("raft: leader asks to overwrite committed entry %d", e.Index))
				return nil, n.failed
			}
			if err := l.truncateFrom(e.Index); err != nil {
				n.fatal(err)
				return nil, err
			}
		}
		toAppend = req.Entries[i:]
		break
	}
	if err := l.append(toAppend); err != nil {
		n.fatal(err)
		return nil, err
	}
	lastNew := req.PrevIndex + uint64(len(req.Entries))
	if req.Commit > n.commit {
		c := req.Commit
		if lastNew < c {
			c = lastNew
		}
		if c > n.commit {
			n.commit = c
			n.cond.Broadcast()
		}
	}
	resp.Success = true
	return resp, nil
}

func (n *Node) abortRestore() {
	if n.restoring != nil {
		if n.restoring.started {
			n.cfg.SM.RestoreAbort()
		}
		n.restoring = nil
		n.cond.Broadcast()
	}
}

func (n *Node) HandleInstallSnapshot(req *SnapReq) (*SnapResp, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if n.failed != nil {
		return nil, n.failed
	}
	if req.Term < n.term {
		return &SnapResp{Term: n.term}, nil
	}
	if req.Term > n.term || n.role != Follower {
		n.stepDown(req.Term)
	}
	n.leader, n.lastContact = req.Leader, time.Now()
	n.resetElectionTimer()
	resp := &SnapResp{Term: n.term}

	if n.applied >= req.SnapIndex && n.restoring == nil {
		resp.OK, resp.Skip = true, true // already that far; leader continues with entries
		return resp, nil
	}
	if req.Seq == 0 {
		n.abortRestore()
		s := &restoreSession{id: req.Session, snapIdx: req.SnapIndex, snapTerm: req.SnapTerm}
		n.restoring = s // pauses the apply loop
		if err := n.waitUntil(time.Now().Add(30*time.Second), func() bool { return !n.applying || n.restoring != s }); err != nil {
			return resp, nil
		}
		if n.restoring != s {
			return resp, nil
		}
		if err := n.cfg.SM.RestoreBegin(); err != nil {
			n.restoring = nil
			n.fatal(err)
			return nil, err
		}
		s.started = true
	}
	s := n.restoring
	if s == nil || s.id != req.Session || s.nextSeq != req.Seq || !s.started {
		return resp, nil
	}
	if err := n.cfg.SM.RestoreChunk(req.Items); err != nil {
		n.abortRestore()
		n.fatal(err)
		return nil, err
	}
	s.nextSeq++
	if req.Done {
		if err := n.cfg.SM.RestoreFinish(s.snapIdx, s.snapTerm); err != nil {
			n.restoring = nil
			n.fatal(err)
			return nil, err
		}
		if err := n.log.reset(s.snapIdx, s.snapTerm); err != nil {
			n.fatal(err)
			return nil, err
		}
		if s.snapIdx > n.commit {
			n.commit = s.snapIdx
		}
		n.applied, n.smApplied = s.snapIdx, s.snapIdx
		n.restoring = nil
		n.cfg.Logf("[%s] installed snapshot at %d", n.cfg.ID, s.snapIdx)
		n.cond.Broadcast()
	}
	resp.OK = true
	return resp, nil
}

// ---- log replication: leader side ----

func (n *Node) advanceCommit() {
	m := make([]uint64, 0, len(n.others)+1)
	m = append(m, n.log.lastIndex()) // own log is durable
	for _, p := range n.others {
		m = append(m, n.match[p])
	}
	sort.Slice(m, func(i, j int) bool { return m[i] > m[j] })
	c := m[n.quorum()-1]
	if c > n.commit {
		if t, ok := n.log.term(c); ok && t == n.term { // only current-term entries commit by counting
			n.commit = c
			n.cond.Broadcast()
		}
	}
}

func (n *Node) replicator(peer string) {
	defer n.wg.Done()
	tick := time.NewTicker(n.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.kick[peer]:
		case <-tick.C:
		}
		for n.replicateOnce(peer) {
		}
	}
}

// replicateOnce sends one AppendEntries (or a whole snapshot) to peer and
// reports whether more should be sent right away.
func (n *Node) replicateOnce(peer string) bool {
	n.mu.Lock()
	if n.role != Leader || n.failed != nil || n.stopped {
		n.mu.Unlock()
		return false
	}
	term, next := n.term, n.next[peer]
	if next <= n.log.prevIndex { // the entry before next was compacted away
		n.mu.Unlock()
		return n.sendSnapshot(peer, term)
	}
	prevIndex := next - 1
	prevTerm, _ := n.log.term(prevIndex)
	entries := n.log.slice(next, n.cfg.MaxBatchEntries, n.cfg.MaxBatchBytes)
	req := &AppendReq{Term: term, Leader: n.cfg.ID, PrevIndex: prevIndex, PrevTerm: prevTerm, Entries: entries, Commit: n.commit}
	sentAt := time.Now()
	n.mu.Unlock()

	resp, err := n.cfg.Transport.AppendEntries(peer, req)

	n.mu.Lock()
	defer n.mu.Unlock()
	if err != nil || resp == nil || n.role != Leader || n.term != term {
		return false
	}
	if resp.Term > n.term {
		n.stepDown(resp.Term)
		return false
	}
	if sentAt.After(n.ackTime[peer]) {
		n.ackTime[peer] = sentAt
		n.cond.Broadcast()
	}
	if resp.Success {
		if m := prevIndex + uint64(len(entries)); m > n.match[peer] {
			n.match[peer] = m
		}
		n.next[peer] = n.match[peer] + 1
		n.advanceCommit()
		return n.next[peer] <= n.log.lastIndex()
	}
	nx := resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		if i := n.log.lastIndexOfTerm(resp.ConflictTerm); i != 0 {
			nx = i + 1
		}
	}
	if nx < 1 {
		nx = 1
	}
	if nx >= next {
		nx = next - 1
	}
	if nx < 1 {
		nx = 1
	}
	n.next[peer] = nx
	return true
}

// sendSnapshot streams the state machine to a follower that fell behind the
// retained log. The scan is fuzzy: it runs while new commands keep being
// applied, so the snapshot is anchored at the applied index at the start and
// the log after that index (pinned against compaction) repairs it on replay.
func (n *Node) sendSnapshot(peer string, term uint64) bool {
	n.mu.Lock()
	if n.role != Leader || n.term != term {
		n.mu.Unlock()
		return false
	}
	snapIndex := n.smApplied
	snapTerm, ok := n.log.term(snapIndex)
	if !ok {
		n.mu.Unlock()
		return false
	}
	n.snapPin[peer] = snapIndex
	session := n.rnd.Uint64()
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		if n.snapPin != nil {
			delete(n.snapPin, peer)
		}
		n.mu.Unlock()
	}()

	var cursor []byte
	for seq := uint64(0); ; seq++ {
		items, nextCursor, done, err := n.cfg.SM.SnapshotScan(cursor, n.cfg.SnapshotChunkBytes)
		if err != nil {
			n.cfg.Logf("[%s] snapshot scan: %v", n.cfg.ID, err)
			return false
		}
		sentAt := time.Now()
		kv := make([]KV, len(items))
		copy(kv, items)
		resp, err := n.cfg.Transport.InstallSnapshot(peer, &SnapReq{
			Term: term, Leader: n.cfg.ID, Session: session, Seq: seq,
			SnapIndex: snapIndex, SnapTerm: snapTerm, Items: kv, Done: done,
		})
		n.mu.Lock()
		if err != nil || resp == nil || n.role != Leader || n.term != term || n.stopped {
			n.mu.Unlock()
			return false
		}
		if resp.Term > n.term {
			n.stepDown(resp.Term)
			n.mu.Unlock()
			return false
		}
		if sentAt.After(n.ackTime[peer]) {
			n.ackTime[peer] = sentAt
			n.cond.Broadcast()
		}
		if !resp.OK {
			n.mu.Unlock()
			return false
		}
		if done || resp.Skip {
			if snapIndex > n.match[peer] && !resp.Skip {
				n.match[peer] = snapIndex
			}
			if n.next[peer] <= snapIndex {
				n.next[peer] = snapIndex + 1
			}
			n.mu.Unlock()
			return true
		}
		n.mu.Unlock()
		cursor = nextCursor
	}
}

// ---- proposals ----

// Propose replicates cmd and returns once it was committed and applied on
// this node. After ErrTimeout or a NotLeaderError the command may still end
// up committed, so only idempotent commands are safe to retry blindly.
func (n *Node) Propose(cmd []byte, timeout time.Duration) error {
	if len(cmd) == 0 {
		return errors.New("raft: empty command")
	}
	if len(cmd) > n.cfg.MaxCommandBytes {
		return ErrTooBig
	}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return ErrStopped
	}
	if n.failed != nil {
		err := n.failed
		n.mu.Unlock()
		return err
	}
	if n.role != Leader {
		err := n.notLeader()
		n.mu.Unlock()
		return err
	}
	n.mu.Unlock()

	p := &proposal{data: cmd, done: make(chan error, 1)}
	n.propMu.Lock()
	n.propQ = append(n.propQ, p)
	n.propMu.Unlock()
	select {
	case n.propSig <- struct{}{}:
	default:
	}
	select {
	case err := <-p.done:
		return err
	case <-time.After(timeout):
		return ErrTimeout
	case <-n.stopCh:
		return ErrStopped
	}
}

func (n *Node) failQueued(err error) {
	n.propMu.Lock()
	q := n.propQ
	n.propQ = nil
	n.propMu.Unlock()
	for _, p := range q {
		p.done <- err
	}
}

// appendLoop turns queued proposals into log entries: everything queued while
// the previous batch was being fsynced goes out with a single fsync.
func (n *Node) appendLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.propSig:
		}
		n.propMu.Lock()
		q := n.propQ
		n.propQ = nil
		n.propMu.Unlock()
		if len(q) == 0 {
			continue
		}
		n.mu.Lock()
		if n.role != Leader || n.failed != nil {
			err := error(n.notLeader())
			if n.failed != nil {
				err = n.failed
			}
			n.mu.Unlock()
			for _, p := range q {
				p.done <- err
			}
			continue
		}
		entries := make([]Entry, len(q))
		idx := n.log.lastIndex()
		for i, p := range q {
			idx++
			p.term, p.index = n.term, idx
			entries[i] = Entry{Term: n.term, Index: idx, Data: p.data}
		}
		if err := n.log.append(entries); err != nil {
			n.fatal(err)
			n.mu.Unlock()
			for _, p := range q {
				p.done <- err
			}
			continue
		}
		for _, p := range q {
			n.waiters[p.index] = p
		}
		n.advanceCommit() // single-node clusters commit here
		n.kickAll()
		n.mu.Unlock()
	}
}

// ---- applying ----

const maxApplyBatch = 1024

func (n *Node) applyLoop() {
	defer n.wg.Done()
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for !n.stopped && n.failed == nil && !(n.commit > n.applied && n.restoring == nil) {
			n.cond.Wait()
		}
		if n.stopped || n.failed != nil {
			return
		}
		lo, hi := n.applied+1, n.commit
		if hi-lo >= maxApplyBatch {
			hi = lo + maxApplyBatch - 1
		}
		var cmds [][]byte
		for _, e := range n.log.slice(lo, int(hi-lo+1), 1<<62) {
			if e.Data != nil {
				cmds = append(cmds, e.Data)
			}
		}
		term, _ := n.log.term(hi)
		n.applying = true
		n.mu.Unlock()
		var err error
		if len(cmds) > 0 {
			err = n.cfg.SM.Apply(hi, term, cmds)
		}
		n.mu.Lock()
		n.applying = false
		if err != nil {
			n.fatal(fmt.Errorf("raft: apply: %w", err))
			return
		}
		if n.restoring != nil || n.applied >= hi { // a snapshot replaced the state meanwhile
			n.cond.Broadcast()
			continue
		}
		n.applied = hi
		if len(cmds) > 0 {
			n.smApplied = hi
		}
		n.notifyApplied()
		n.maybeCompact()
		n.cond.Broadcast()
	}
}

func (n *Node) notifyApplied() {
	for idx, p := range n.waiters {
		if idx > n.applied {
			continue
		}
		if t, ok := n.log.term(idx); ok && t == p.term {
			p.done <- nil
		} else {
			p.done <- n.notLeader()
		}
		delete(n.waiters, idx)
	}
}

func (n *Node) maybeCompact() {
	target := n.smApplied
	if target <= uint64(n.cfg.LogRetain) {
		return
	}
	target -= uint64(n.cfg.LogRetain)
	for _, pin := range n.snapPin {
		if pin < target {
			target = pin
		}
	}
	if target > n.log.prevIndex {
		if err := n.log.compactTo(target); err != nil {
			n.fatal(err)
		}
	}
}

// ---- linearizable reads ----

// ReadBarrier returns once this node, still being the leader, has applied
// everything committed before the call (the ReadIndex protocol). Reads served
// from the state machine afterwards are linearizable.
func (n *Node) ReadBarrier(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return n.notLeader()
	}
	term := n.term
	// The commit index is only trustworthy once an entry of this term committed.
	if err := n.waitUntil(deadline, func() bool {
		t, _ := n.log.term(n.commit)
		return n.role != Leader || n.term != term || t == term
	}); err != nil {
		return err
	}
	if n.role != Leader || n.term != term {
		return n.notLeader()
	}
	readIdx := n.commit
	t0 := time.Now()
	n.kickAll()
	confirmed := func() bool {
		c := 1
		for _, p := range n.others {
			if !n.ackTime[p].Before(t0) {
				c++
			}
		}
		return c >= n.quorum()
	}
	if err := n.waitUntil(deadline, func() bool {
		return n.role != Leader || n.term != term || (confirmed() && n.applied >= readIdx)
	}); err != nil {
		return err
	}
	if n.role != Leader || n.term != term {
		return n.notLeader()
	}
	return nil
}
