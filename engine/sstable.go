package engine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
)

// File layout:
//
//	[data block | crc32c]...  [bloom]  [index]  [footer]
//
// A data block is a run of encoded entries (~blockSize bytes) in key order.
// The index holds, per block, its first key, offset and length (incl. crc).
// Footer (40 B): indexOff, indexLen, bloomOff, bloomLen, magic (u64 LE).
const (
	blockSize  = 4096
	footerSize = 40
	sstMagic   = 0x6b76737374303031
)

func sstPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.sst", id))
}

type indexEntry struct {
	firstKey []byte
	off, len uint64
}

// ---- writer ----

type sstWriter struct {
	f          *os.File
	w          *bufio.Writer
	off        uint64
	block      []byte
	blockFirst []byte
	index      []indexEntry
	hashes     []uint64
	lastKey    []byte
	count      int
}

func newSSTWriter(path string) (*sstWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &sstWriter{f: f, w: bufio.NewWriterSize(f, 64<<10)}, nil
}

func (w *sstWriter) write(p []byte) error {
	n, err := w.w.Write(p)
	w.off += uint64(n)
	return err
}

func (w *sstWriter) add(e entry) error {
	if w.count > 0 && bytes.Compare(e.key, w.lastKey) <= 0 {
		return fmt.Errorf("sstable: keys not strictly ascending")
	}
	if len(w.block) == 0 {
		w.blockFirst = bytes.Clone(e.key)
	}
	w.block = appendEntry(w.block, e)
	w.hashes = append(w.hashes, hashKey(e.key))
	w.lastKey = append(w.lastKey[:0], e.key...)
	w.count++
	if len(w.block) >= blockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *sstWriter) flushBlock() error {
	if len(w.block) == 0 {
		return nil
	}
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.Checksum(w.block, castagnoli))
	start := w.off
	if err := w.write(w.block); err != nil {
		return err
	}
	if err := w.write(crc[:]); err != nil {
		return err
	}
	w.index = append(w.index, indexEntry{firstKey: w.blockFirst, off: start, len: w.off - start})
	w.block = w.block[:0]
	return nil
}

func (w *sstWriter) finish() error {
	if err := w.flushBlock(); err != nil {
		return err
	}
	bloom := buildBloom(w.hashes)
	bloomOff := w.off
	if err := w.write(bloom); err != nil {
		return err
	}
	var idx []byte
	idx = binary.AppendUvarint(idx, uint64(len(w.index)))
	for _, ie := range w.index {
		idx = binary.AppendUvarint(idx, uint64(len(ie.firstKey)))
		idx = append(idx, ie.firstKey...)
		idx = binary.AppendUvarint(idx, ie.off)
		idx = binary.AppendUvarint(idx, ie.len)
	}
	indexOff := w.off
	if err := w.write(idx); err != nil {
		return err
	}
	var foot [footerSize]byte
	binary.LittleEndian.PutUint64(foot[0:], indexOff)
	binary.LittleEndian.PutUint64(foot[8:], uint64(len(idx)))
	binary.LittleEndian.PutUint64(foot[16:], bloomOff)
	binary.LittleEndian.PutUint64(foot[24:], uint64(len(bloom)))
	binary.LittleEndian.PutUint64(foot[32:], sstMagic)
	if err := w.write(foot[:]); err != nil {
		return err
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

func (w *sstWriter) abort() { w.f.Close() }

// writeTable drains it into a new durable SSTable and opens it.
func writeTable(dir string, id uint64, it iterator) (*sstable, error) {
	path := sstPath(dir, id)
	w, err := newSSTWriter(path)
	if err != nil {
		return nil, err
	}
	for ; it.Valid(); it.Next() {
		if err := w.add(it.Entry()); err != nil {
			w.abort()
			os.Remove(path)
			return nil, err
		}
	}
	if err := it.Err(); err != nil {
		w.abort()
		os.Remove(path)
		return nil, err
	}
	if err := w.finish(); err != nil {
		w.abort()
		os.Remove(path)
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		return nil, err
	}
	return openSSTable(path, id)
}

// ---- reader ----

type sstable struct {
	f     *os.File
	id    uint64
	index []indexEntry
	bloom []byte
}

func openSSTable(path string, id uint64) (*sstable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	t, err := loadSSTable(f, id)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return t, nil
}

func loadSSTable(f *os.File, id uint64) (*sstable, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < footerSize {
		return nil, errCorrupt
	}
	var foot [footerSize]byte
	if _, err := f.ReadAt(foot[:], st.Size()-footerSize); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint64(foot[32:]) != sstMagic {
		return nil, errCorrupt
	}
	indexOff := binary.LittleEndian.Uint64(foot[0:])
	indexLen := binary.LittleEndian.Uint64(foot[8:])
	bloomOff := binary.LittleEndian.Uint64(foot[16:])
	bloomLen := binary.LittleEndian.Uint64(foot[24:])
	limit := uint64(st.Size()) - footerSize
	if indexOff+indexLen > limit || bloomOff+bloomLen > limit {
		return nil, errCorrupt
	}
	t := &sstable{f: f, id: id}
	t.bloom = make([]byte, bloomLen)
	if _, err := f.ReadAt(t.bloom, int64(bloomOff)); err != nil {
		return nil, err
	}
	idx := make([]byte, indexLen)
	if _, err := f.ReadAt(idx, int64(indexOff)); err != nil {
		return nil, err
	}
	cnt, n := binary.Uvarint(idx)
	if n <= 0 {
		return nil, errCorrupt
	}
	idx = idx[n:]
	for i := uint64(0); i < cnt; i++ {
		kl, n := binary.Uvarint(idx)
		if n <= 0 || kl > uint64(len(idx)-n) {
			return nil, errCorrupt
		}
		idx = idx[n:]
		ie := indexEntry{firstKey: bytes.Clone(idx[:kl])}
		idx = idx[kl:]
		if ie.off, n = binary.Uvarint(idx); n <= 0 {
			return nil, errCorrupt
		}
		idx = idx[n:]
		if ie.len, n = binary.Uvarint(idx); n <= 0 || ie.len < 4 || ie.off+ie.len > indexOff {
			return nil, errCorrupt
		}
		idx = idx[n:]
		t.index = append(t.index, ie)
	}
	return t, nil
}

func (t *sstable) close() error { return t.f.Close() }

func (t *sstable) readBlock(i int) ([]entry, error) {
	ie := t.index[i]
	buf := make([]byte, ie.len)
	if _, err := t.f.ReadAt(buf, int64(ie.off)); err != nil {
		return nil, err
	}
	body := buf[:len(buf)-4]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return nil, fmt.Errorf("sstable %06d block %d: %w", t.id, i, errCorrupt)
	}
	var ents []entry
	for len(body) > 0 {
		e, n, err := decodeEntry(body)
		if err != nil {
			return nil, err
		}
		ents = append(ents, e)
		body = body[n:]
	}
	return ents, nil
}

// blockFor returns the index of the only block that may contain key, or -1.
func (t *sstable) blockFor(key []byte) int {
	return sort.Search(len(t.index), func(i int) bool {
		return bytes.Compare(t.index[i].firstKey, key) > 0
	}) - 1
}

func (t *sstable) get(key []byte) (entry, bool, error) {
	if !bloomMayContain(t.bloom, hashKey(key)) {
		return entry{}, false, nil
	}
	b := t.blockFor(key)
	if b < 0 {
		return entry{}, false, nil
	}
	ents, err := t.readBlock(b)
	if err != nil {
		return entry{}, false, err
	}
	j := sort.Search(len(ents), func(j int) bool { return bytes.Compare(ents[j].key, key) >= 0 })
	if j < len(ents) && bytes.Equal(ents[j].key, key) {
		return ents[j], true, nil
	}
	return entry{}, false, nil
}

type sstIter struct {
	t    *sstable
	bi   int
	ents []entry
	pos  int
	err  error
}

func (t *sstable) iter(start []byte) *sstIter {
	it := &sstIter{t: t}
	if start != nil {
		if b := t.blockFor(start); b > 0 {
			it.bi = b
		}
	}
	it.bi-- // loadNext pre-increments
	it.loadNext()
	for start != nil && it.Valid() && bytes.Compare(it.Entry().key, start) < 0 {
		it.Next()
	}
	return it
}

func (it *sstIter) loadNext() {
	for {
		it.bi++
		it.ents, it.pos = nil, 0
		if it.bi >= len(it.t.index) {
			return
		}
		ents, err := it.t.readBlock(it.bi)
		if err != nil {
			it.err = err
			return
		}
		if len(ents) > 0 {
			it.ents = ents
			return
		}
	}
}

func (it *sstIter) Valid() bool  { return it.err == nil && it.pos < len(it.ents) }
func (it *sstIter) Entry() entry { return it.ents[it.pos] }
func (it *sstIter) Err() error   { return it.err }
func (it *sstIter) Next() {
	it.pos++
	if it.pos >= len(it.ents) {
		it.loadNext()
	}
}
