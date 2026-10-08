package engine

import (
	"bytes"
	"sort"
	"sync/atomic"
)

// The tables of a DB are organised in levels (leveled compaction, the rolling
// merge of the LSM-tree paper):
//
//   - level 0 holds freshly flushed memtables; files may overlap each other and
//     are ordered oldest -> newest;
//   - levels 1.. hold sorted, NON-overlapping files; every level is ~10x larger
//     than the previous one, and shallower levels always hold newer data.
//
// A compaction merges a few files of level L with the files of level L+1 that
// overlap them, so its cost is bounded (a few tens of MiB) no matter how large
// the database is. This keeps write stalls short and latency predictable.
const numLevels = 7

// version is an immutable snapshot of the table layout. Readers pin one with a
// single reference count instead of touching every table; a version holds a
// reference on each of its tables, which keeps their files alive until the
// last reader of any version containing them is done.
type version struct {
	levels [numLevels][]*sstable
	refs   atomic.Int32
}

func newVersion(levels [numLevels][]*sstable) *version {
	v := &version{levels: levels}
	v.refs.Store(1)
	for _, files := range levels {
		for _, t := range files {
			t.ref()
		}
	}
	return v
}

func (v *version) ref() { v.refs.Add(1) }

func (v *version) unref() {
	if v.refs.Add(-1) == 0 {
		for _, files := range v.levels {
			for _, t := range files {
				t.unref()
			}
		}
	}
}

func (v *version) levelBytes(l int) int64 {
	var n int64
	for _, t := range v.levels[l] {
		n += t.size
	}
	return n
}

func (v *version) manifest(logNum uint64) manifest {
	m := manifest{logNum: logNum}
	for l, files := range v.levels {
		for _, t := range files {
			m.levels[l] = append(m.levels[l], t.id)
		}
	}
	return m
}

// withChanges returns the level layout with remove taken out and add inserted.
// Level 0 keeps its order (new files appended); other levels stay sorted by key.
func (v *version) withChanges(remove map[*sstable]bool, add [numLevels][]*sstable) [numLevels][]*sstable {
	var out [numLevels][]*sstable
	for l := range out {
		files := make([]*sstable, 0, len(v.levels[l])+len(add[l]))
		for _, t := range v.levels[l] {
			if !remove[t] {
				files = append(files, t)
			}
		}
		files = append(files, add[l]...)
		if l > 0 {
			sort.Slice(files, func(i, j int) bool { return bytes.Compare(files[i].smallest, files[j].smallest) < 0 })
		}
		out[l] = files
	}
	return out
}

// get returns the newest entry for key stored in the tables.
func (v *version) get(key []byte) (entry, bool, error) {
	l0 := v.levels[0]
	for i := len(l0) - 1; i >= 0; i-- {
		t := l0[i]
		if t.covers(key) {
			if e, ok, err := t.get(key); err != nil || ok {
				return e, ok, err
			}
		}
	}
	for l := 1; l < numLevels; l++ {
		files := v.levels[l]
		i := sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].largest, key) >= 0 })
		if i < len(files) && files[i].covers(key) {
			if e, ok, err := files[i].get(key); err != nil || ok {
				return e, ok, err
			}
		}
	}
	return entry{}, false, nil
}

// iterators returns one iterator per level-0 file (newest first) and one per
// deeper level, in order of decreasing recency, positioned at start.
func (v *version) iterators(start []byte, fill bool) []iterator {
	var its []iterator
	l0 := v.levels[0]
	for i := len(l0) - 1; i >= 0; i-- {
		if start == nil || bytes.Compare(l0[i].largest, start) >= 0 {
			its = append(its, l0[i].iterOpt(start, fill))
		}
	}
	for l := 1; l < numLevels; l++ {
		if len(v.levels[l]) > 0 {
			its = append(its, newLevelIter(v.levels[l], start, fill))
		}
	}
	return its
}

func (t *sstable) covers(key []byte) bool {
	return t.smallest != nil && bytes.Compare(key, t.smallest) >= 0 && bytes.Compare(key, t.largest) <= 0
}

// overlapping returns the files of a sorted, disjoint level that intersect [lo, hi].
func overlapping(files []*sstable, lo, hi []byte) []*sstable {
	i := sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].largest, lo) >= 0 })
	j := i
	for j < len(files) && bytes.Compare(files[j].smallest, hi) <= 0 {
		j++
	}
	return files[i:j]
}

// levelIter walks the files of one sorted level as a single iterator, opening
// each file only when the scan reaches it.
type levelIter struct {
	files []*sstable
	idx   int
	cur   *sstIter
	fill  bool
	err   error
}

func newLevelIter(files []*sstable, start []byte, fill bool) *levelIter {
	it := &levelIter{files: files, fill: fill}
	if start != nil {
		it.idx = sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].largest, start) >= 0 })
	}
	it.open(start)
	return it
}

// open positions on the file at idx (skipping exhausted ones).
func (it *levelIter) open(start []byte) {
	for it.idx < len(it.files) {
		it.cur = it.files[it.idx].iterOpt(start, it.fill)
		if it.cur.Valid() {
			return
		}
		if err := it.cur.Err(); err != nil {
			it.err = err
			return
		}
		it.idx++
		start = nil
	}
	it.cur = nil
}

func (it *levelIter) Valid() bool  { return it.err == nil && it.cur != nil && it.cur.Valid() }
func (it *levelIter) Entry() entry { return it.cur.Entry() }
func (it *levelIter) Err() error   { return it.err }
func (it *levelIter) Next() {
	it.cur.Next()
	if it.cur.Valid() {
		return
	}
	if err := it.cur.Err(); err != nil {
		it.err = err
		return
	}
	it.idx++
	it.open(nil)
}
