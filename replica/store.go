package replica

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"kvstore/engine"
	"kvstore/kvnet"
	"kvstore/raft"
)

// Store is the replicated key/value store: it implements kvnet.Store. Writes
// go through Raft; reads use ReadIndex, so every successful call is
// linearizable. On a follower every call fails with kvnet.NotLeaderError
// carrying the leader's client address, which clients follow.
type Store struct {
	node    *raft.Node
	sm      *KVMachine
	timeout time.Duration

	mu          sync.RWMutex
	clientAddrs map[string]string // node ID -> client-facing address
}

func NewStore(node *raft.Node, sm *KVMachine, timeout time.Duration) *Store {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Store{node: node, sm: sm, timeout: timeout, clientAddrs: map[string]string{}}
}

// SetClientAddrs tells the store where each node serves clients, so that
// not-leader replies can point at the leader.
func (s *Store) SetClientAddrs(m map[string]string) {
	s.mu.Lock()
	s.clientAddrs = m
	s.mu.Unlock()
}

// translate converts Raft errors into what clients should do about them.
func (s *Store) translate(err error) error {
	var nl *raft.NotLeaderError
	switch {
	case err == nil, errors.Is(err, engine.ErrNotFound):
		return err
	case errors.As(err, &nl):
		s.mu.RLock()
		addr := s.clientAddrs[nl.Leader]
		s.mu.RUnlock()
		return &kvnet.NotLeaderError{Leader: addr}
	case errors.Is(err, raft.ErrTimeout), errors.Is(err, raft.ErrStopped):
		// No quorum / shutting down: tell the client to look elsewhere and retry.
		return &kvnet.NotLeaderError{}
	}
	return err
}

func checkKey(k []byte) error {
	if len(k) == 0 {
		return engine.ErrEmptyKey
	}
	if isReserved(k) {
		return fmt.Errorf("replica: keys starting with %q are reserved", reservedPrefix)
	}
	return nil
}

func (s *Store) propose(ops []engine.Op) error {
	for _, o := range ops {
		if err := checkKey(o.Key); err != nil {
			return err
		}
	}
	return s.translate(s.node.Propose(EncodeCommand(ops), s.timeout))
}

func (s *Store) Put(key, value []byte) error {
	return s.propose([]engine.Op{{Key: key, Value: value}})
}

func (s *Store) Delete(key []byte) error {
	return s.propose([]engine.Op{{Key: key, Delete: true}})
}

func (s *Store) BatchPut(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return fmt.Errorf("replica: %d keys but %d values", len(keys), len(values))
	}
	ops := make([]engine.Op, len(keys))
	for i := range keys {
		ops[i] = engine.Op{Key: keys[i], Value: values[i]}
	}
	return s.propose(ops)
}

func (s *Store) Read(key []byte) ([]byte, error) {
	if err := s.node.ReadBarrier(s.timeout); err != nil {
		return nil, s.translate(err)
	}
	return s.sm.Get(key)
}

func (s *Store) ReadKeyRange(start, end []byte) ([]engine.KV, error) {
	if err := s.node.ReadBarrier(s.timeout); err != nil {
		return nil, s.translate(err)
	}
	return s.sm.Range(start, end)
}
