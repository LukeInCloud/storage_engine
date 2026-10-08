// Package engine is a persistent, sorted key/value storage engine built as an
// LSM tree: WAL -> memtable -> immutable SSTables, with a manifest tying the
// files together, size-tiered background compaction, group commit and a block
// cache.
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
	// CompactionTrigger is the number of SSTables that triggers a merge of that
	// many adjacent tables. Default 4; negative disables compaction.
	CompactionTrigger int
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
	tables  []*sstable // oldest -> newest
	man     manifest
	nextNum uint64
	closed  bool
	failed  error

	flushCh   chan struct{}
	compactCh chan struct{}
	flushWG   sync.WaitGroup
	compactWG sync.WaitGroup
}

type writeReq struct {
	batch []entry
	err   error
	done  chan struct{}
}

func Open(dir string, opts *Options) (*DB, error) {
	db := &DB{dir: dir, flushCh: make(chan struct{}, 1), compactCh: make(chan struct{}, 1)}
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
	for _, id := range man.tables {
		inManifest[id] = true
		if id > maxNum {
			maxNum = id
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
	for _, id := range man.tables {
		t, err := openSSTable(sstPath(db.dir, id), id, db.cache)
		if err != nil {
			return err
		}
		db.tables = append(db.tables, t)
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
			return err
		}
		db.tables = append(db.tables, t)
		man.tables = append(man.tables, id)
	}
	man.logNum = walNum
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
func (db *DB) write(batch []entry) error {
	if len(batch) == 0 {
		return nil
	}
	req := &writeReq{batch: batch, done: make(chan struct{})}
	db.qmu.Lock()
	db.queue = append(db.queue, req)
	if db.leading {
		db.qmu.Unlock()
		<-req.done
		return req.err
	}
	db.leading = true
	db.qmu.Unlock()
	for {
		db.qmu.Lock()
		reqs := db.queue
		db.queue = nil
		if len(reqs) == 0 {
			db.leading = false
			db.qmu.Unlock()
			break
		}
		db.qmu.Unlock()
		err := db.commit(reqs)
		for _, r := range reqs {
			r.err = err
			close(r.done)
		}
	}
	<-req.done
	return req.err
}

func (db *DB) memFull() bool { return db.mem.approxSize() >= db.opts.MemtableSize }

// stalled reports that compaction is too far behind. Caller holds db.mu.
func (db *DB) stalled() bool {
	n := db.compactTrigger()
	return n >= 2 && len(db.tables) >= 4*n
}

func (db *DB) commit(reqs []*writeReq) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// Backpressure keeps latency predictable: when the memtable is full, wait
	// for the previous flush and for compaction to catch up rather than let
	// memory or the number of files (read cost) grow without bound.
	db.mu.Lock()
	for !db.closed && db.failed == nil && db.memFull() && (db.imm != nil || db.stalled()) {
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
	man := manifest{logNum: liveWAL, tables: append(append([]uint64(nil), db.man.tables...), id)}
	if err := writeManifest(db.dir, man); err != nil {
		db.mu.Unlock()
		return err
	}
	db.man = man
	db.tables = append(db.tables, t)
	db.imm, db.immWAL = nil, nil
	db.cond.Broadcast()
	db.mu.Unlock()
	db.kickCompaction()

	oldWAL.close()
	os.Remove(walPath(db.dir, oldWAL.num))
	return nil
}

// ---- read path ----

type snapshot struct {
	mem, imm *memtable
	tables   []*sstable // referenced; call release when done
}

func (s snapshot) release() {
	for _, t := range s.tables {
		t.unref()
	}
}

func (db *DB) snapshot() (snapshot, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return snapshot{}, ErrClosed
	}
	tables := append([]*sstable(nil), db.tables...)
	for _, t := range tables {
		t.ref()
	}
	return snapshot{db.mem, db.imm, tables}, nil
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
	for i := len(s.tables) - 1; i >= 0; i-- {
		e, ok, err := s.tables[i].get(key)
		if err != nil {
			return nil, err
		}
		if ok {
			return found(e)
		}
	}
	return nil, ErrNotFound
}

// ReadKeyRange returns all live pairs with start <= key <= end in key order.
// A nil start means "from the first key"; a nil end means "to the last key".
func (db *DB) ReadKeyRange(start, end []byte) ([]KV, error) {
	s, err := db.snapshot()
	if err != nil {
		return nil, err
	}
	defer s.release()
	srcs := []iterator{s.mem.iter(start)}
	if s.imm != nil {
		srcs = append(srcs, s.imm.iter(start))
	}
	for i := len(s.tables) - 1; i >= 0; i-- {
		srcs = append(srcs, s.tables[i].iter(start))
	}
	m := newMergeIter(srcs)
	var out []KV
	for ; m.Valid(); m.Next() {
		e := m.Entry()
		if end != nil && bytes.Compare(e.key, end) > 0 {
			break
		}
		if e.kind == kindPut {
			out = append(out, KV{bytes.Clone(e.key), bytes.Clone(e.value)})
		}
	}
	return out, m.Err()
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
	for _, t := range db.tables {
		t.unref()
	}
	return first
}
