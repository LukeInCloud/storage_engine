// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout)
// with a durable log, pre-vote, ReadIndex linearizable reads and log
// compaction with fuzzy snapshots, on top of a pluggable state machine and
// transport.
//
// Membership is static: the set of node IDs is fixed in Config.
package raft

import (
	"errors"
	"fmt"
)

type Entry struct {
	Term, Index uint64
	Data        []byte // nil = no-op (appended by a new leader)
}

type KV struct{ Key, Value []byte }

type VoteReq struct {
	Term         uint64
	Candidate    string
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
}

type VoteResp struct {
	Term    uint64
	Granted bool
}

type AppendReq struct {
	Term      uint64
	Leader    string
	PrevIndex uint64
	PrevTerm  uint64
	Entries   []Entry
	Commit    uint64
}

type AppendResp struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64 // hint for the leader on failure
	ConflictTerm  uint64
}

// SnapReq carries one chunk of a (fuzzy) snapshot of the state machine.
// Chunks of a session arrive in order (Seq 0,1,2...); Done marks the last one.
type SnapReq struct {
	Term      uint64
	Leader    string
	Session   uint64
	Seq       uint64
	SnapIndex uint64 // log index the snapshot is anchored at
	SnapTerm  uint64
	Items     []KV
	Done      bool
}

type SnapResp struct {
	Term uint64
	OK   bool
	Skip bool // follower is already at or beyond SnapIndex; stop streaming
}

// Transport delivers RPCs to peers. Implementations must enforce their own
// timeouts and may return an error for any failure.
type Transport interface {
	RequestVote(to string, req *VoteReq) (*VoteResp, error)
	AppendEntries(to string, req *AppendReq) (*AppendResp, error)
	InstallSnapshot(to string, req *SnapReq) (*SnapResp, error)
}

// StateMachine is the replicated application.
//
// Commands must be deterministic and *idempotent when replayed in order*:
// snapshots are fuzzy (taken while the state keeps changing) and are repaired
// by replaying the log after the snapshot index, which converges only for
// such commands (e.g. puts and deletes).
type StateMachine interface {
	// Applied returns the index/term of the last durably applied command.
	Applied() (index, term uint64, err error)
	// Apply atomically and durably applies cmds (in order) and records
	// (index, term) as applied. cmds may be empty.
	Apply(index, term uint64, cmds [][]byte) error

	// SnapshotScan streams the state: pass nil first, then next, until done.
	SnapshotScan(cursor []byte, maxBytes int) (items []KV, next []byte, done bool, err error)
	// RestoreBegin starts building a replacement state off to the side.
	RestoreBegin() error
	RestoreChunk(items []KV) error
	// RestoreFinish atomically swaps the new state in, marking it applied at (index, term).
	RestoreFinish(index, term uint64) error
	RestoreAbort()
}

var (
	ErrTimeout = errors.New("raft: timed out")
	ErrStopped = errors.New("raft: node stopped")
	ErrTooBig  = errors.New("raft: command too large")
)

// NotLeaderError is returned when the node cannot serve the request. Leader
// is the ID of the current leader if known.
type NotLeaderError struct{ Leader string }

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("raft: not the leader (leader=%q)", e.Leader)
}
