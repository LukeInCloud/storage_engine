package engine

import (
	"bytes"
	"container/heap"
)

type mergeItem struct {
	it  iterator
	pri int // lower = newer source, wins ties
}

type mergeHeap []mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].it.Entry().key, h[j].it.Entry().key); c != 0 {
		return c < 0
	}
	return h[i].pri < h[j].pri
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// mergeIter merges sources ordered newest-first into one ascending stream,
// yielding only the newest version of each key (tombstones included).
type mergeIter struct {
	h   mergeHeap
	err error
}

func newMergeIter(srcs []iterator) *mergeIter {
	m := &mergeIter{}
	for i, s := range srcs {
		m.add(mergeItem{it: s, pri: i})
	}
	heap.Init(&m.h)
	return m
}

func (m *mergeIter) add(it mergeItem) {
	if it.it.Valid() {
		m.h = append(m.h, it)
	} else if err := it.it.Err(); err != nil && m.err == nil {
		m.err = err
	}
}

func (m *mergeIter) Valid() bool  { return m.err == nil && len(m.h) > 0 }
func (m *mergeIter) Entry() entry { return m.h[0].it.Entry() }
func (m *mergeIter) Err() error   { return m.err }

func (m *mergeIter) Next() {
	key := bytes.Clone(m.h[0].it.Entry().key)
	for len(m.h) > 0 && bytes.Equal(m.h[0].it.Entry().key, key) {
		top := heap.Pop(&m.h).(mergeItem)
		top.it.Next()
		if top.it.Valid() {
			heap.Push(&m.h, top)
		} else if err := top.it.Err(); err != nil && m.err == nil {
			m.err = err
		}
	}
}
