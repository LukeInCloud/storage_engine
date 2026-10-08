package engine

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	maxHeight    = 12
	nodeOverhead = 48
)

type node struct {
	e    entry
	next []*node
}

// memtable is an in-memory sorted skip list. Writes replace in place, so it
// holds only the newest version of every key.
type memtable struct {
	mu     sync.RWMutex
	head   *node
	height int
	size   int
	count  int
	rnd    *rand.Rand
}

func newMemtable() *memtable {
	return &memtable{
		head:   &node{next: make([]*node, maxHeight)},
		height: 1,
		rnd:    rand.New(rand.NewSource(1)),
	}
}

func (m *memtable) approxSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *memtable) len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

func (m *memtable) randomHeight() int {
	h := 1
	for h < maxHeight && m.rnd.Intn(4) == 0 {
		h++
	}
	return h
}

func (m *memtable) put(key, value []byte, kind byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var prev [maxHeight]*node
	x := m.head
	for i := m.height - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].e.key, key) < 0 {
			x = x.next[i]
		}
		prev[i] = x
	}
	if n := prev[0].next[0]; n != nil && bytes.Equal(n.e.key, key) {
		m.size += len(value) - len(n.e.value)
		n.e.value = bytes.Clone(value)
		n.e.kind = kind
		return
	}
	h := m.randomHeight()
	if h > m.height {
		for i := m.height; i < h; i++ {
			prev[i] = m.head
		}
		m.height = h
	}
	n := &node{
		e:    entry{key: bytes.Clone(key), value: bytes.Clone(value), kind: kind},
		next: make([]*node, h),
	}
	for i := 0; i < h; i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}
	m.size += len(key) + len(value) + nodeOverhead
	m.count++
}

// seek returns the first node with key >= start (or the first node if start is nil).
// Caller must hold m.mu.
func (m *memtable) seek(start []byte) *node {
	if start == nil {
		return m.head.next[0]
	}
	x := m.head
	for i := m.height - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].e.key, start) < 0 {
			x = x.next[i]
		}
	}
	return x.next[0]
}

func (m *memtable) get(key []byte) (entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := m.seek(key)
	if n != nil && bytes.Equal(n.e.key, key) {
		return n.e, true
	}
	return entry{}, false
}

type memIter struct {
	m   *memtable
	cur *node
}

func (m *memtable) iter(start []byte) *memIter {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &memIter{m: m, cur: m.seek(start)}
}

func (it *memIter) Valid() bool { return it.cur != nil }
func (it *memIter) Err() error  { return nil }
func (it *memIter) Entry() entry {
	it.m.mu.RLock()
	defer it.m.mu.RUnlock()
	return it.cur.e
}
func (it *memIter) Next() {
	it.m.mu.RLock()
	it.cur = it.cur.next[0]
	it.m.mu.RUnlock()
}
