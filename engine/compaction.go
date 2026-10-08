package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"
)

var errAborted = errors.New("engine: compaction aborted")

const (
	defaultTargetFileSize = 8 << 20
	defaultLevelBase      = 64 << 20 // maximum size of level 1
	levelMultiplier       = 10
)

func (db *DB) maxBytesForLevel(l int) int64 {
	n := db.opts.LevelBaseBytes
	for i := 1; i < l; i++ {
		n *= levelMultiplier
	}
	return n
}

func (db *DB) compactionEnabled() bool { return db.opts.CompactionTrigger >= 2 }

// compaction describes one unit of work: merge the files `from` of level
// `level` with the overlapping files `into` of level+1.
type compaction struct {
	level   int
	from    []*sstable // newest first for level 0
	into    []*sstable
	trivial bool // a single file with no overlap just moves down, no rewrite
}

// pickCompaction chooses the most urgent job, or nil. Level 0 is scored by file
// count (reads must probe every L0 file), deeper levels by size over capacity.
func (db *DB) pickCompaction(v *version) *compaction {
	best, bestScore := -1, 0.0
	if s := float64(len(v.levels[0])) / float64(db.opts.CompactionTrigger); s >= 1 {
		best, bestScore = 0, s
	}
	for l := 1; l < numLevels-1; l++ {
		if s := float64(v.levelBytes(l)) / float64(db.maxBytesForLevel(l)); s >= 1 && s > bestScore {
			best, bestScore = l, s
		}
	}
	if best < 0 {
		return nil
	}
	return db.compactionFor(v, best)
}

// compactionFor builds the job that pushes data out of level l.
func (db *DB) compactionFor(v *version, l int) *compaction {
	files := v.levels[l]
	if len(files) == 0 {
		return nil
	}
	c := &compaction{level: l}
	if l == 0 {
		for i := len(files) - 1; i >= 0; i-- { // newest first
			c.from = append(c.from, files[i])
		}
	} else {
		// Round-robin through the key space so every file gets its turn.
		pick := files[0]
		for _, f := range files {
			if bytes.Compare(f.smallest, db.compactPtr[l]) > 0 {
				pick = f
				break
			}
		}
		c.from = []*sstable{pick}
	}
	lo, hi := c.from[0].smallest, c.from[0].largest
	for _, f := range c.from {
		if bytes.Compare(f.smallest, lo) < 0 {
			lo = f.smallest
		}
		if bytes.Compare(f.largest, hi) > 0 {
			hi = f.largest
		}
	}
	c.into = overlapping(v.levels[l+1], lo, hi)
	c.trivial = len(c.into) == 0 && len(c.from) == 1
	return c
}

func (c *compaction) keyRange() (lo, hi []byte) {
	lo, hi = c.from[0].smallest, c.from[0].largest
	for _, f := range append(append([]*sstable(nil), c.from...), c.into...) {
		if bytes.Compare(f.smallest, lo) < 0 {
			lo = f.smallest
		}
		if bytes.Compare(f.largest, hi) > 0 {
			hi = f.largest
		}
	}
	return lo, hi
}

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
	if !db.compactionEnabled() {
		return false, nil
	}
	db.compactMu.Lock()
	defer db.compactMu.Unlock()
	v, ok := db.pinVersion()
	if !ok {
		return false, nil
	}
	defer v.unref()
	c := db.pickCompaction(v)
	if c == nil {
		return false, nil
	}
	return true, db.runCompaction(v, c)
}

// pinVersion returns the current version with a reference held.
func (db *DB) pinVersion() (*version, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed || db.failed != nil {
		return nil, false
	}
	db.cur.ref()
	return db.cur, true
}

func (db *DB) allocNum() uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	n := db.nextNum
	db.nextNum++
	return n
}

func (db *DB) runCompaction(v *version, c *compaction) error {
	t0 := time.Now()
	var read, wrote int64
	defer func() { db.ctr.compactNanos.Add(int64(time.Since(t0))) }()
	remove := map[*sstable]bool{}
	for _, f := range c.from {
		remove[f] = true
	}
	for _, f := range c.into {
		remove[f] = true
	}
	var add [numLevels][]*sstable
	var outputs []*sstable

	if c.trivial {
		add[c.level+1] = c.from // same files, one level down
		remove = map[*sstable]bool{c.from[0]: true}
	} else {
		lo, hi := c.keyRange()
		bottom := true // no deeper level has data for this key range
		for l := c.level + 2; l < numLevels && bottom; l++ {
			if len(overlapping(v.levels[l], lo, hi)) > 0 {
				bottom = false
			}
		}
		var srcs []iterator
		for _, f := range c.from { // newest first
			srcs = append(srcs, f.iterOpt(nil, false))
		}
		if len(c.into) > 0 {
			srcs = append(srcs, newLevelIter(c.into, nil, false))
		}
		it := &compactIter{src: newMergeIter(srcs), dropTombs: bottom, stop: db.isClosed}
		it.skip()
		var err error
		if outputs, err = db.writeOutputs(it); err != nil {
			return err
		}
		add[c.level+1] = outputs
		for t := range remove {
			read += t.size
		}
		for _, t := range outputs {
			wrote += t.size
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed || db.failed != nil {
		db.discard(outputs)
		return errAborted
	}
	// Flushes only add level-0 files while we worked; our inputs must still be there.
	present := map[*sstable]bool{}
	for _, files := range db.cur.levels {
		for _, t := range files {
			present[t] = true
		}
	}
	for t := range remove {
		if !present[t] {
			db.discard(outputs)
			return fmt.Errorf("engine: compaction input %06d vanished", t.id)
		}
	}
	levels := db.cur.withChanges(remove, add)
	if err := db.installVersion(levels, db.man.logNum, outputs, remove, c.trivial); err != nil {
		db.discard(outputs)
		return err
	}
	if c.level > 0 {
		db.compactPtr[c.level] = c.from[len(c.from)-1].largest
	}
	if c.trivial {
		db.ctr.trivialMoves.Add(1)
	} else {
		db.ctr.compactions.Add(1)
		db.ctr.compactBytesRead.Add(read)
		db.ctr.compactBytesWrite.Add(wrote)
	}
	return nil
}

// installVersion makes levels current. Caller holds db.mu. The manifest is
// written first, so a crash leaves either the old or the new layout. fresh
// are newly created tables whose creator reference is handed to the version;
// removed tables are deleted once their last reader is gone (unless moved).
func (db *DB) installVersion(levels [numLevels][]*sstable, logNum uint64, fresh []*sstable, removed map[*sstable]bool, moved bool) error {
	nv := newVersion(levels)
	man := nv.manifest(logNum)
	if err := writeManifest(db.dir, man); err != nil {
		nv.unref()
		return err
	}
	db.man = man
	if !moved {
		for t := range removed {
			t.obsolete.Store(true)
		}
	}
	old := db.cur
	db.cur = nv
	old.unref()
	for _, t := range fresh {
		t.unref() // creator's reference; the version holds its own
	}
	db.cond.Broadcast()
	return nil
}

// discard deletes output files that never became part of a version.
func (db *DB) discard(outs []*sstable) {
	for _, t := range outs {
		t.obsolete.Store(true)
		t.unref()
	}
}

// writeOutputs writes the merged stream as files of about TargetFileSize each.
func (db *DB) writeOutputs(it iterator) ([]*sstable, error) {
	var outs []*sstable
	var w *sstWriter
	var path string
	var id uint64
	fail := func(err error) ([]*sstable, error) {
		if w != nil {
			w.abort()
			os.Remove(path)
		}
		db.discard(outs)
		return nil, err
	}
	seal := func() error {
		t, err := finishTable(w, path, id, db.cache)
		w = nil
		if err != nil {
			return err
		}
		outs = append(outs, t)
		return nil
	}
	for ; it.Valid(); it.Next() {
		if w == nil {
			id = db.allocNum()
			path = sstPath(db.dir, id)
			var err error
			if w, err = newSSTWriter(path); err != nil {
				return fail(err)
			}
		}
		if err := w.add(it.Entry()); err != nil {
			return fail(err)
		}
		if w.size() >= int64(db.opts.TargetFileSize) {
			if err := seal(); err != nil {
				return fail(err)
			}
		}
	}
	if err := it.Err(); err != nil {
		return fail(err)
	}
	if w != nil {
		if err := seal(); err != nil {
			return fail(err)
		}
	}
	if len(outs) > 0 {
		if err := syncDir(db.dir); err != nil {
			return fail(err)
		}
	}
	return outs, nil
}

func (db *DB) isClosed() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.closed
}

// CompactAll pushes all data down to the deepest level, dropping overwritten
// versions and tombstones. It is a maintenance operation (reclaims space);
// normal operation never needs it.
func (db *DB) CompactAll() error {
	if !db.compactionEnabled() {
		return errors.New("engine: compaction disabled")
	}
	if err := db.Flush(); err != nil {
		return err
	}
	db.compactMu.Lock()
	defer db.compactMu.Unlock()
	for l := 0; l < numLevels-1; l++ {
		for {
			v, ok := db.pinVersion()
			if !ok {
				return ErrClosed
			}
			if len(v.levels[l]) == 0 {
				v.unref()
				break
			}
			c := db.compactionFor(v, l)
			c.trivial = false // always rewrite so tombstones and old versions are dropped
			err := db.runCompaction(v, c)
			v.unref()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// levelFiles reports the number of tables per level (diagnostics and tests).
func (db *DB) levelFiles() [numLevels]int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var n [numLevels]int
	for l, f := range db.cur.levels {
		n[l] = len(f)
	}
	return n
}

// checkLevels verifies the structural invariants of the layout.
func (db *DB) checkLevels() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for l := 1; l < numLevels; l++ {
		files := db.cur.levels[l]
		for i := range files {
			if bytes.Compare(files[i].smallest, files[i].largest) > 0 {
				return fmt.Errorf("level %d file %06d: smallest > largest", l, files[i].id)
			}
			if i > 0 && bytes.Compare(files[i-1].largest, files[i].smallest) >= 0 {
				return fmt.Errorf("level %d: files %06d and %06d overlap", l, files[i-1].id, files[i].id)
			}
		}
	}
	return nil
}
