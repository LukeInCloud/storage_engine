// Package engine is a persistent, sorted key/value storage engine built as an
// LSM tree: WAL -> memtable -> immutable SSTables organised in levels, with a
// manifest tying the files together, leveled background compaction with
// bounded-size merges, group commit and a block cache.
package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("engine: key not found")
	ErrEmptyKey = errors.New("engine: empty key")
	ErrClosed   = errors.New("engine: closed")
)

type Options struct {
	// MemtableSize is the approximate size at which the memtable is flushed
	// to an SSTable. Default 4 MiB.
	MemtableSize int
	// NoSync disables fsync on every write. Faster, but a power loss may drop
	// recent writes. Default (false) = every acknowledged write is durable.
	NoSync bool
	// BlockCacheSize is the capacity in bytes of the decoded-block LRU cache.
	// Default 32 MiB; negative disables the cache.
	BlockCacheSize int
	// CompactionTrigger is the number of level-0 files that triggers a
	// compaction into level 1. Writes are slowed down at twice this number and
	// stopped at three times it. Default 4; negative disables compaction.
	CompactionTrigger int
	// TargetFileSize is the approximate size of SSTables produced by
	// compaction. Default 8 MiB.
	TargetFileSize int
	// LevelBaseBytes is the capacity of level 1; each deeper level holds 10x
	// more. Default 64 MiB.
	LevelBaseBytes int64
}

type KV struct{ Key, Value []byte }

type DB struct {
	dir  string
	opts Options

	writeMu sync.Mutex // held while a commit group is applied; also by Close

	qmu     sync.Mutex // group commit queue
	queue   []*writeReq
	leading bool

	cache *blockCache

	mu      sync.RWMutex // guards the fields below
	cond    *sync.Cond   // signalled when imm is flushed or on failure
	mem     *memtable
	imm     *memtable // being flushed, read-only
	wal     *wal
	immWAL  *wal
	cur     *version // current table layout
	man     manifest
	nextNum uint64
	closed  bool
	failed  error

	ctr        counters
	thr        throttle
	quit       chan struct{} // closed by Close; stops auxiliary goroutines
	bgWG       sync.WaitGroup
	compactMu  sync.Mutex // one compaction (or CompactAll) at a time
	compactPtr [numLevels][]byte

	flushCh   chan struct{}
	compactCh chan struct{}
	flushWG   sync.WaitGroup
	compactWG sync.WaitGroup
}

type writeReq struct {
	batch []entry
	err   error
	done  chan struct{} // closed when the batch is committed
	lead  chan struct{} // receives a token when this writer must lead the next round
}

func Open(dir string, opts *Options) (*DB, error) {
	db := &DB{dir: dir, flushCh: make(chan struct{}, 1), compactCh: make(chan struct{}, 1), quit: make(chan struct{})}
	if opts != nil {
		db.opts = *opts
	}
	if db.opts.MemtableSize <= 0 {
		db.opts.MemtableSize = 4 << 20
	}
	if db.opts.BlockCacheSize == 0 {
		db.opts.BlockCacheSize = 32 << 20
	}
	if db.opts.CompactionTrigger == 0 {
		db.opts.CompactionTrigger = 4
	}
	if db.opts.TargetFileSize <= 0 {
		db.opts.TargetFileSize = defaultTargetFileSize
	}
	if db.opts.LevelBaseBytes <= 0 {
		db.opts.LevelBaseBytes = defaultLevelBase
	}
	db.cache = newBlockCache(db.opts.BlockCacheSize)
	db.cond = sync.NewCond(&db.mu)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := db.recover(); err != nil {
		db.closeFiles()
		return nil, err
	}
	db.flushWG.Add(1)
	go db.flushLoop()
	db.compactWG.Add(1)
	go db.compactLoop()
	db.bgWG.Add(1)
	go db.throttleLoop()
	db.kickCompaction()
	return db, nil
}

func (db *DB) recover() error {
	man, err := readManifest(db.dir)
	if err != nil {
		return err
	}
	files, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}
	inManifest := map[uint64]bool{}
	maxNum := man.logNum
	for _, ids := range man.levels {
		for _, id := range ids {
			inManifest[id] = true
			if id > maxNum {
				maxNum = id
			}
		}
	}
	var walNums []uint64
	for _, f := range files {
		name := f.Name()
		switch {
		case name == manifestName+".tmp":
			os.Remove(db.dir + "/" + name)
		case strings.HasSuffix(name, ".sst"):
			id, err := strconv.ParseUint(strings.TrimSuffix(name, ".sst"), 10, 64)
			if err != nil {
				continue
			}
			if id > maxNum {
				maxNum = id
			}
			if !inManifest[id] { // orphan from a flush that crashed before the manifest update
				os.Remove(db.dir + "/" + name)
			}
		case strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log"):
			n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log"), 10, 64)
			if err != nil {
				continue
			}
			if n > maxNum {
				maxNum = n
			}
			walNums = append(walNums, n)
		}
	}
	var levels [numLevels][]*sstable
	opened := func() {
		for _, files := range levels {
			for _, t := range files {
				t.unref()
			}
		}
	}
	for l, ids := range man.levels {
		for _, id := range ids {
			t, err := openSSTable(sstPath(db.dir, id), id, db.cache)
			if err != nil {
				opened()
				return err
			}
			levels[l] = append(levels[l], t)
		}
		if l > 0 {
			files := levels[l]
			sort.Slice(files, func(i, j int) bool { return bytes.Compare(files[i].smallest, files[j].smallest) < 0 })
		}
	}

	// Replay WALs that were not yet flushed.
	sort.Slice(walNums, func(i, j int) bool { return walNums[i] < walNums[j] })
	replayed := newMemtable()
	for _, n := range walNums {
		if n < man.logNum {
			continue
		}
		err := replayWAL(walPath(db.dir, n), func(batch []entry) {
			for _, e := range batch {
				replayed.put(e.key, e.value, e.kind)
			}
		})
		if err != nil {
			return err
		}
	}

	walNum := maxNum + 1
	db.nextNum = walNum + 1
	if replayed.len() > 0 {
		// Persist recovered data right away so old WALs can be dropped.
		id := db.nextNum
		db.nextNum++
		t, err := writeTable(db.dir, id, replayed.iter(nil), db.cache)
		if err != nil {
			opened()
			return err
		}
		levels[0] = append(levels[0], t)
	}
	db.cur = newVersion(levels)
	opened() // the version now holds its own references
	man = db.cur.manifest(walNum)
	if err := writeManifest(db.dir, man); err != nil {
		return err
	}
	db.man = man
	for _, n := range walNums {
		os.Remove(walPath(db.dir, n))
	}
	w, err := createWAL(db.dir, walNum, !db.opts.NoSync)
	if err != nil {
		return err
	}
	db.wal = w
	db.mem = newMemtable()
	return nil
}

// ---- write path ----

func (db *DB) Put(key, value []byte) error {
	return db.BatchPut([][]byte{key}, [][]byte{value})
}

// BatchPut applies all pairs atomically (one WAL record) and durably.
func (db *DB) BatchPut(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return fmt.Errorf("engine: %d keys but %d values", len(keys), len(values))
	}
	batch := make([]entry, len(keys))
	for i := range keys {
		if len(keys[i]) == 0 {
			return ErrEmptyKey
		}
		batch[i] = entry{key: keys[i], value: values[i], kind: kindPut}
	}
	return db.write(batch)
}

// Op is one element of an atomic mixed batch.
type Op struct {
	Key, Value []byte
	Delete     bool
}

// Write applies puts and deletes atomically (one WAL record).
func (db *DB) Write(ops []Op) error {
	batch := make([]entry, len(ops))
	for i, o := range ops {
		if len(o.Key) == 0 {
			return ErrEmptyKey
		}
		batch[i] = entry{key: o.Key, value: o.Value, kind: kindPut}
		if o.Delete {
			batch[i] = entry{key: o.Key, kind: kindDelete}
		}
	}
	return db.write(batch)
}

// Delete writes a tombstone. Deleting a missing key is not an error.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	return db.write([]entry{{key: key, kind: kindDelete}})
}

// write implements group commit. Concurrent writers queue their batch; the
// first one becomes the leader and commits everything queued so far with a
// single WAL write and a single fsync, then wakes the others. While the
// leader is in fsync, new writers accumulate for the next round.
//
// A leader runs exactly one round (which includes its own batch) and then
// hands leadership to the oldest queued writer. Looping until the queue is
// empty would starve the leader's own caller under continuous load.
func (db *DB) write(batch []entry) error {
	if len(batch) == 0 {
		return nil
	}
	req := &writeReq{batch: batch, done: make(chan struct{}), lead: make(chan struct{}, 1)}
	db.qmu.Lock()
	db.queue = append(db.queue, req)
	if db.leading {
		db.qmu.Unlock()
		select {
		case <-req.done: // committed by another leader
			return req.err
		case <-req.lead: // promoted: our batch is still queued
		}
	} else {
		db.leading = true
		db.qmu.Unlock()
	}

	db.qmu.Lock()
	reqs := db.queue
	db.queue = nil
	db.qmu.Unlock()
	err := db.commit(reqs)
	for _, r := range reqs {
		r.err = err
		close(r.done)
	}

	db.qmu.Lock()
	if len(db.queue) > 0 {
		next := db.queue[0] // leadership stays "taken" and moves to next
		db.qmu.Unlock()
		next.lead <- struct{}{}
	} else {
		db.leading = false
		db.qmu.Unlock()
	}
	return req.err
}

func (db *DB) memFull() bool { return db.mem.approxSize() >= db.opts.MemtableSize }

// stalled reports that compaction is too far behind. Caller holds db.mu.
func (db *DB) stalled() bool {
	return db.compactionEnabled() && len(db.cur.levels[0]) >= 3*db.opts.CompactionTrigger
}

func (db *DB) commit(reqs []*writeReq) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// Adaptive brake: admit bytes at the rate compaction can sustain, so the
	// hard stall below is a last resort rather than the normal regulator.
	size := 0
	for _, r := range reqs {
		for _, e := range r.batch {
			size += len(e.key) + len(e.value)
		}
	}
	if d := db.thr.wait(size); d > 0 {
		db.ctr.slowdowns.Add(1)
		db.ctr.slowdownNanos.Add(int64(d))
	}

	// Backpressure keeps latency predictable: when the memtable is full, wait
	// for the previous flush and for compaction to catch up rather than let
	// memory or the number of files (read cost) grow without bound.
	db.mu.Lock()
	if !db.closed && db.failed == nil && db.memFull() && (db.imm != nil || db.stalled()) {
		t0 := time.Now()
		for !db.closed && db.failed == nil && db.memFull() && (db.imm != nil || db.stalled()) {
			db.cond.Wait()
		}
		db.ctr.stalls.Add(1)
		db.ctr.stallNanos.Add(int64(time.Since(t0)))
	}
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.failed != nil {
		err := db.failed
		db.mu.Unlock()
		return err
	}
	db.mu.Unlock()

	if db.memFull() {
		if err := db.rotate(); err != nil {
			return db.fail(err)
		}
	}
	batches := make([][]entry, len(reqs))
	for i, r := range reqs {
		batches[i] = r.batch
	}
	if err := db.wal.append(batches...); err != nil {
		// A partial record may sit in the log; refuse further writes.
		return db.fail(err)
	}
	for _, b := range batches {
		for _, e := range b {
			db.mem.put(e.key, e.value, e.kind)
		}
	}
	db.ctr.writtenBytes.Add(int64(size))
	return nil
}

func (db *DB) fail(err error) error {
	db.mu.Lock()
	if db.failed == nil {
		db.failed = err
	}
	db.cond.Broadcast()
	db.mu.Unlock()
	return err
}

// rotate freezes the memtable and starts a fresh one with its own WAL.
// Caller holds writeMu and has verified imm == nil.
func (db *DB) rotate() error {
	db.mu.Lock()
	num := db.nextNum
	db.nextNum++
	db.mu.Unlock()
	w, err := createWAL(db.dir, num, !db.opts.NoSync)
	if err != nil {
		return err
	}
	db.mu.Lock()
	old := db.wal
	db.imm, db.immWAL = db.mem, old
	db.mem, db.wal = newMemtable(), w
	db.mu.Unlock()
	select {
	case db.flushCh <- struct{}{}:
	default:
	}
	return nil
}

func (db *DB) kickCompaction() {
	select {
	case db.compactCh <- struct{}{}:
	default:
	}
}

func (db *DB) flushLoop() {
	defer db.flushWG.Done()
	for range db.flushCh {
		if err := db.flushImm(); err != nil {
			db.fail(err)
		}
	}
}

func (db *DB) flushImm() error {
	db.mu.Lock()
	imm, oldWAL := db.imm, db.immWAL
	if imm == nil {
		db.mu.Unlock()
		return nil
	}
	id := db.nextNum
	db.nextNum++
	liveWAL := db.wal.num // WAL paired with the current memtable
	db.mu.Unlock()

	t, err := writeTable(db.dir, id, imm.iter(nil), db.cache)
	if err != nil {
		return err
	}
	db.mu.Lock()
	var add [numLevels][]*sstable
	add[0] = []*sstable{t}
	if err := db.installVersion(db.cur.withChanges(nil, add), liveWAL, []*sstable{t}, nil, false); err != nil {
		db.mu.Unlock()
		return err
	}
	db.imm, db.immWAL = nil, nil
	db.cond.Broadcast()
	db.mu.Unlock()
	db.ctr.flushes.Add(1)
	db.ctr.flushBytes.Add(t.size)
	db.kickCompaction()

	oldWAL.close()
	os.Remove(walPath(db.dir, oldWAL.num))
	return nil
}

// ---- read path ----

type snapshot struct {
	mem, imm *memtable
	v        *version // pinned; call release when done
}

func (s snapshot) release() { s.v.unref() }

func (db *DB) snapshot() (snapshot, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return snapshot{}, ErrClosed
	}
	db.cur.ref()
	return snapshot{db.mem, db.imm, db.cur}, nil
}

// Read returns the value for key or ErrNotFound.
func (db *DB) Read(key []byte) ([]byte, error) {
	s, err := db.snapshot()
	if err != nil {
		return nil, err
	}
	defer s.release()
	found := func(e entry) ([]byte, error) {
		if e.kind == kindDelete {
			return nil, ErrNotFound
		}
		return bytes.Clone(e.value), nil
	}
	if e, ok := s.mem.get(key); ok {
		return found(e)
	}
	if s.imm != nil {
		if e, ok := s.imm.get(key); ok {
			return found(e)
		}
	}
	e, ok, err := s.v.get(key)
	if err != nil {
		return nil, err
	}
	if ok {
		return found(e)
	}
	return nil, ErrNotFound
}

// ReadKeyRange returns all live pairs with start <= key <= end in key order.
// A nil start means "from the first key"; a nil end means "to the last key".
func (db *DB) ReadKeyRange(start, end []byte) ([]KV, error) {
	kvs, _, err := db.scan(start, end, 0)
	return kvs, err
}

// Scan returns live pairs with key >= start in key order, stopping once about
// maxBytes of keys+values were collected (at least one pair is returned if any
// exist). more reports whether pairs remain; continue from lastKey+"\x00".
// It lets callers stream a dataset larger than RAM.
func (db *DB) Scan(start []byte, maxBytes int) (kvs []KV, more bool, err error) {
	return db.scan(start, nil, maxBytes)
}

func (db *DB) scan(start, end []byte, maxBytes int) ([]KV, bool, error) {
	s, err := db.snapshot()
	if err != nil {
		return nil, false, err
	}
	defer s.release()
	srcs := []iterator{s.mem.iter(start)}
	if s.imm != nil {
		srcs = append(srcs, s.imm.iter(start))
	}
	srcs = append(srcs, s.v.iterators(start, true)...)
	m := newMergeIter(srcs)
	var out []KV
	size := 0
	for ; m.Valid(); m.Next() {
		e := m.Entry()
		if end != nil && bytes.Compare(e.key, end) > 0 {
			break
		}
		if e.kind != kindPut {
			continue
		}
		if maxBytes > 0 && len(out) > 0 && size >= maxBytes {
			return out, true, m.Err()
		}
		out = append(out, KV{bytes.Clone(e.key), bytes.Clone(e.value)})
		size += len(e.key) + len(e.value)
	}
	return out, false, m.Err()
}

// Sync makes everything written so far durable. It is only needed with
// Options.NoSync, where the caller batches durability itself (as the
// replicated state machine does: its durability comes from the Raft log).
func (db *DB) Sync() error {
	db.mu.RLock()
	closed := db.closed
	w, iw := db.wal, db.immWAL
	db.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	for _, x := range []*wal{iw, w} { // the frozen WAL first: it holds the older data
		if x == nil {
			continue
		}
		// A WAL closed meanwhile was flushed into an SSTable, which is already durable.
		if err := x.f.Sync(); err != nil && !errors.Is(err, os.ErrClosed) {
			return err
		}
	}
	return nil
}

// Flush writes the memtable to an SSTable and returns when it is durable in
// the table layout (the WAL alone already guarantees durability; this is for
// maintenance such as CompactAll).
func (db *DB) Flush() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.mu.Lock()
	for !db.closed && db.failed == nil && db.imm != nil {
		db.cond.Wait()
	}
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.failed != nil {
		err := db.failed
		db.mu.Unlock()
		return err
	}
	empty := db.mem.len() == 0
	db.mu.Unlock()
	if empty {
		return nil
	}
	if err := db.rotate(); err != nil {
		return db.fail(err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	for !db.closed && db.failed == nil && db.imm != nil {
		db.cond.Wait()
	}
	if db.failed != nil {
		return db.failed
	}
	return nil
}

// ---- lifecycle ----

// Close waits for a pending flush and releases all files. Unflushed memtable
// data stays in the WAL and is recovered on the next Open.
func (db *DB) Close() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	db.closed = true
	db.cond.Broadcast()
	db.mu.Unlock()
	close(db.quit)
	db.bgWG.Wait()
	close(db.flushCh)
	db.flushWG.Wait()
	close(db.compactCh)
	db.compactWG.Wait()
	return db.closeFiles()
}

func (db *DB) closeFiles() error {
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	if db.wal != nil {
		keep(db.wal.close())
	}
	if db.cur != nil {
		db.cur.unref()
	}
	return first
}
