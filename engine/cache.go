package engine

import (
	"container/list"
	"sync"
)

type cacheKey struct {
	id    uint64
	block int
}

type cacheVal struct {
	key  cacheKey
	ents []entry
	size int
}

// blockCache is a byte-bounded LRU of decoded SSTable blocks. A nil cache is
// valid and caches nothing. File ids are never reused, so entries of deleted
// tables are never hit again and simply age out.
type blockCache struct {
	mu   sync.Mutex
	cap  int
	used int
	ll   *list.List
	m    map[cacheKey]*list.Element
}

func newBlockCache(capacity int) *blockCache {
	if capacity <= 0 {
		return nil
	}
	return &blockCache{cap: capacity, ll: list.New(), m: map[cacheKey]*list.Element{}}
}

func (c *blockCache) get(k cacheKey) ([]entry, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[k]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*cacheVal).ents, true
	}
	return nil, false
}

func (c *blockCache) put(k cacheKey, ents []entry, size int) {
	if c == nil || size > c.cap {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.m[k]; ok {
		return
	}
	c.m[k] = c.ll.PushFront(&cacheVal{k, ents, size})
	c.used += size
	for c.used > c.cap {
		el := c.ll.Back()
		v := el.Value.(*cacheVal)
		c.ll.Remove(el)
		delete(c.m, v.key)
		c.used -= v.size
	}
}
