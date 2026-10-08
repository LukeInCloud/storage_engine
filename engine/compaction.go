package engine

import "errors"

var errAborted = errors.New("engine: compaction aborted")

// Size-tiered compaction. Tables are ordered oldest -> newest and a newer
// table shadows an older one, so merging a *contiguous* run of tables into one
// table placed at the same position preserves every read result.
//
// Policy: when there are at least CompactionTrigger tables, merge the run of
// CompactionTrigger adjacent tables with the smallest total size (cheapest
// write amplification). Tombstones are dropped only when the run contains the
// oldest table, because only then no older version can be resurrected.

// compactIter filters a merged stream: optionally drops tombstones and checks
// for shutdown every few entries so Close is never held up by a long merge.
type compactIter struct {
	src       *mergeIter
	dropTombs bool
	stop      func() bool
	n         int
	err       error
}

func (c *compactIter) skip() {
	for c.src.Valid() && c.dropTombs && c.src.Entry().kind == kindDelete {
		c.src.Next()
	}
}

func (c *compactIter) Valid() bool {
	if c.err != nil {
		return false
	}
	return c.src.Valid()
}
func (c *compactIter) Entry() entry { return c.src.Entry() }
func (c *compactIter) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.src.Err()
}
func (c *compactIter) Next() {
	c.src.Next()
	c.skip()
	if c.n++; c.n%1024 == 0 && c.stop() {
		c.err = errAborted
	}
}

func (db *DB) compactTrigger() int { return db.opts.CompactionTrigger }

// pickWindow returns the start index of the cheapest run of n adjacent tables.
// Caller holds db.mu.
func pickWindow(tables []*sstable, n int) int {
	best, bestSize := 0, int64(-1)
	for i := 0; i+n <= len(tables); i++ {
		var sum int64
		for _, t := range tables[i : i+n] {
			sum += t.size
		}
		if bestSize < 0 || sum < bestSize {
			best, bestSize = i, sum
		}
	}
	return best
}

func (db *DB) compactLoop() {
	defer db.compactWG.Done()
	for range db.compactCh {
		for {
			did, err := db.compactOnce()
			if err != nil {
				if err != errAborted {
					db.fail(err)
				}
				break
			}
			if !did {
				break
			}
		}
	}
}

func (db *DB) compactOnce() (bool, error) {
	n := db.compactTrigger()
	if n < 2 {
		return false, nil
	}
	db.mu.Lock()
	if db.closed || db.failed != nil || len(db.tables) < n {
		db.mu.Unlock()
		return false, nil
	}
	pos := pickWindow(db.tables, n)
	window := append([]*sstable(nil), db.tables[pos:pos+n]...)
	for _, t := range window {
		t.ref()
	}
	id := db.nextNum
	db.nextNum++
	db.mu.Unlock()
	defer func() {
		for _, t := range window {
			t.unref()
		}
	}()

	srcs := make([]iterator, 0, n)
	for i := len(window) - 1; i >= 0; i-- { // newest first
		srcs = append(srcs, window[i].iterOpt(nil, false))
	}
	it := &compactIter{
		src:       newMergeIter(srcs),
		dropTombs: pos == 0,
		stop:      db.isClosed,
	}
	it.skip()
	out, err := writeTable(db.dir, id, it, db.cache)
	if err != nil {
		return false, err
	}
	empty := len(out.index) == 0 // everything cancelled out
	if empty {
		out.obsolete.Store(true)
		out.unref()
		out = nil
	}

	db.mu.Lock()
	// Only flushes (which append) run concurrently, so the window is still at pos.
	for i, t := range window {
		if pos+i >= len(db.tables) || db.tables[pos+i] != t {
			db.mu.Unlock()
			if out != nil {
				out.obsolete.Store(true)
				out.unref()
			}
			return false, errors.New("engine: compaction window changed")
		}
	}
	tables := make([]*sstable, 0, len(db.tables)-n+1)
	tables = append(tables, db.tables[:pos]...)
	if out != nil {
		tables = append(tables, out)
	}
	tables = append(tables, db.tables[pos+n:]...)
	man := manifest{logNum: db.man.logNum}
	for _, t := range tables {
		man.tables = append(man.tables, t.id)
	}
	if err := writeManifest(db.dir, man); err != nil {
		db.mu.Unlock()
		return false, err
	}
	db.man, db.tables = man, tables
	db.cond.Broadcast() // wake writers stalled on too many tables
	db.mu.Unlock()

	for _, t := range window {
		t.obsolete.Store(true)
		t.unref() // the DB's own reference
	}
	return true, nil
}

func (db *DB) isClosed() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.closed
}
