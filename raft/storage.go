package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ---- hard state: currentTerm and votedFor ----
// Must be durable before answering any RPC that depends on it.

func saveHardState(dir string, term uint64, votedFor string) error {
	tmp := filepath.Join(dir, "state.tmp")
	data := fmt.Sprintf("term %d\nvote %s\n", term, votedFor)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, "state")); err != nil {
		return err
	}
	return syncDir(dir)
}

func loadHardState(dir string) (term uint64, votedFor string, err error) {
	data, err := os.ReadFile(filepath.Join(dir, "state"))
	if os.IsNotExist(err) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		k, v, _ := strings.Cut(line, " ")
		switch k {
		case "term":
			if term, err = strconv.ParseUint(v, 10, 64); err != nil {
				return 0, "", fmt.Errorf("raft state: %w", err)
			}
		case "vote":
			votedFor = v
		}
	}
	return term, votedFor, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ---- log ----
//
// File: header | record*
//   header: prevIndex(u64) prevTerm(u64) crc32c(u32)   (entries start after prevIndex)
//   record: len(u32) crc32c(u32) | term(u64) index(u64) data
//
// Records are only appended; a conflicting suffix is removed by truncating the
// file. Compaction drops a prefix in memory at once and rewrites the file
// (temp file + rename) once enough garbage has built up.

const logHeaderSize = 20

type logStore struct {
	dir     string
	f       *os.File
	noSync  bool
	end     int64 // append offset
	dropped int   // entries dropped in memory but still in the file

	prevIndex, prevTerm uint64  // entry preceding entries[0]
	entries             []Entry // in-memory window: prevIndex+1 ...
	offs                []int64 // file offset of each entry's record
}

func logPath(dir string) string { return filepath.Join(dir, "raft.log") }

func encodeHeader(prevIndex, prevTerm uint64) []byte {
	h := make([]byte, logHeaderSize)
	binary.LittleEndian.PutUint64(h[0:], prevIndex)
	binary.LittleEndian.PutUint64(h[8:], prevTerm)
	binary.LittleEndian.PutUint32(h[16:], crc32.Checksum(h[:16], crcTable))
	return h
}

func appendRecord(dst []byte, e Entry) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, 8)...)
	dst = binary.LittleEndian.AppendUint64(dst, e.Term)
	dst = binary.LittleEndian.AppendUint64(dst, e.Index)
	dst = append(dst, e.Data...)
	payload := dst[start+8:]
	binary.LittleEndian.PutUint32(dst[start:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(dst[start+4:], crc32.Checksum(payload, crcTable))
	return dst
}

func openLog(dir string, noSync bool) (*logStore, error) {
	os.Remove(logPath(dir) + ".tmp")
	l := &logStore{dir: dir, noSync: noSync}
	data, err := os.ReadFile(logPath(dir))
	if os.IsNotExist(err) {
		if err := l.rewrite(); err != nil {
			return nil, err
		}
		return l, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) < logHeaderSize || crc32.Checksum(data[:16], crcTable) != binary.LittleEndian.Uint32(data[16:]) {
		return nil, fmt.Errorf("raft log: corrupt header")
	}
	l.prevIndex = binary.LittleEndian.Uint64(data[0:])
	l.prevTerm = binary.LittleEndian.Uint64(data[8:])
	pos := logHeaderSize
	next := l.prevIndex + 1
	for len(data)-pos >= 8 {
		n := int(binary.LittleEndian.Uint32(data[pos:]))
		sum := binary.LittleEndian.Uint32(data[pos+4:])
		if n < 16 || len(data)-pos-8 < n {
			break
		}
		payload := data[pos+8 : pos+8+n]
		if crc32.Checksum(payload, crcTable) != sum {
			break
		}
		e := Entry{
			Term:  binary.LittleEndian.Uint64(payload[0:]),
			Index: binary.LittleEndian.Uint64(payload[8:]),
			Data:  append([]byte(nil), payload[16:]...),
		}
		if e.Index != next {
			break
		}
		if len(e.Data) == 0 {
			e.Data = nil
		}
		l.entries = append(l.entries, e)
		l.offs = append(l.offs, int64(pos))
		pos += 8 + n
		next++
	}
	f, err := os.OpenFile(logPath(dir), os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(pos)); err != nil { // drop a torn tail
		f.Close()
		return nil, err
	}
	l.f, l.end = f, int64(pos)
	return l, nil
}

func (l *logStore) close() error {
	if l.f == nil {
		return nil
	}
	return l.f.Close()
}

func (l *logStore) sync() error {
	if l.noSync {
		return nil
	}
	return l.f.Sync()
}

func (l *logStore) lastIndex() uint64 { return l.prevIndex + uint64(len(l.entries)) }

func (l *logStore) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.prevTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// term returns the term of the entry at index, if it is known.
func (l *logStore) term(index uint64) (uint64, bool) {
	switch {
	case index == l.prevIndex:
		return l.prevTerm, true
	case index > l.prevIndex && index <= l.lastIndex():
		return l.entries[index-l.prevIndex-1].Term, true
	}
	return 0, false
}

// slice returns entries from index on, up to maxN entries / maxBytes (at least one).
func (l *logStore) slice(from uint64, maxN, maxBytes int) []Entry {
	if from <= l.prevIndex || from > l.lastIndex() {
		return nil
	}
	i := int(from - l.prevIndex - 1)
	size := 0
	j := i
	for j < len(l.entries) && j-i < maxN {
		size += len(l.entries[j].Data) + 24
		j++
		if size >= maxBytes {
			break
		}
	}
	return l.entries[i:j:j]
}

func (l *logStore) lastIndexOfTerm(term uint64) uint64 {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Term == term {
			return l.entries[i].Index
		}
		if l.entries[i].Term < term {
			break
		}
	}
	return 0
}

func (l *logStore) firstIndexOfTerm(term uint64) uint64 {
	first := uint64(0)
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Term != term {
			if first != 0 {
				break
			}
			continue
		}
		first = l.entries[i].Index
	}
	return first
}

// append durably adds entries; their indices must continue the log.
func (l *logStore) append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	if entries[0].Index != l.lastIndex()+1 {
		return fmt.Errorf("raft log: append at %d, expected %d", entries[0].Index, l.lastIndex()+1)
	}
	var buf []byte
	offs := make([]int64, len(entries))
	for i, e := range entries {
		offs[i] = l.end + int64(len(buf))
		buf = appendRecord(buf, e)
	}
	if _, err := l.f.WriteAt(buf, l.end); err != nil {
		return err
	}
	if err := l.sync(); err != nil {
		return err
	}
	l.end += int64(len(buf))
	l.entries = append(l.entries, entries...)
	l.offs = append(l.offs, offs...)
	return nil
}

// truncateFrom removes entry index and everything after it.
func (l *logStore) truncateFrom(index uint64) error {
	if index <= l.prevIndex || index > l.lastIndex() {
		return nil
	}
	pos := int(index - l.prevIndex - 1)
	off := l.offs[pos]
	if err := l.f.Truncate(off); err != nil {
		return err
	}
	if err := l.sync(); err != nil {
		return err
	}
	l.entries, l.offs, l.end = l.entries[:pos], l.offs[:pos], off
	return nil
}

// compactTo drops entries <= index from the in-memory window.
func (l *logStore) compactTo(index uint64) error {
	if index <= l.prevIndex || index > l.lastIndex() {
		return nil
	}
	cut := int(index - l.prevIndex)
	term := l.entries[cut-1].Term
	l.entries = append([]Entry(nil), l.entries[cut:]...)
	l.offs = append([]int64(nil), l.offs[cut:]...)
	l.prevIndex, l.prevTerm = index, term
	l.dropped += cut
	if l.dropped >= 1024 && l.dropped >= len(l.entries) {
		return l.rewrite()
	}
	return nil
}

// reset discards the whole log; the next entry will have index prevIndex+1.
func (l *logStore) reset(prevIndex, prevTerm uint64) error {
	l.entries, l.offs = nil, nil
	l.prevIndex, l.prevTerm = prevIndex, prevTerm
	return l.rewrite()
}

// rewrite atomically replaces the file with header + current in-memory entries.
func (l *logStore) rewrite() error {
	buf := encodeHeader(l.prevIndex, l.prevTerm)
	offs := make([]int64, len(l.entries))
	for i, e := range l.entries {
		offs[i] = int64(len(buf))
		buf = appendRecord(buf, e)
	}
	tmp := logPath(l.dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := os.Rename(tmp, logPath(l.dir)); err != nil {
		f.Close()
		return err
	}
	if err := syncDir(l.dir); err != nil {
		f.Close()
		return err
	}
	if l.f != nil {
		l.f.Close()
	}
	l.f, l.offs, l.end, l.dropped = f, offs, int64(len(buf)), 0
	return nil
}
