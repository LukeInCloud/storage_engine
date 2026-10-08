package engine

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const walHeaderSize = 8 // payload length (u32) | crc32c of payload (u32)

func walPath(dir string, num uint64) string {
	return filepath.Join(dir, fmt.Sprintf("wal-%06d.log", num))
}

// wal is an append-only log. One record == one batch, so a batch is atomic:
// after a crash it is either replayed completely or not at all.
type wal struct {
	f    *os.File
	num  uint64
	sync bool
	buf  []byte
}

func createWAL(dir string, num uint64, sync bool) (*wal, error) {
	f, err := os.OpenFile(walPath(dir, num), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return &wal{f: f, num: num, sync: sync}, nil
}

// append writes one record per batch with a single write and a single fsync
// (group commit). Each record stays individually atomic.
func (w *wal) append(batches ...[]entry) error {
	buf := w.buf[:0]
	for _, batch := range batches {
		start := len(buf)
		buf = append(buf, make([]byte, walHeaderSize)...)
		buf = binary.AppendUvarint(buf, uint64(len(batch)))
		for _, e := range batch {
			buf = appendEntry(buf, e)
		}
		payload := buf[start+walHeaderSize:]
		binary.LittleEndian.PutUint32(buf[start:], uint32(len(payload)))
		binary.LittleEndian.PutUint32(buf[start+4:], crc32.Checksum(payload, castagnoli))
	}
	w.buf = buf
	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func (w *wal) close() error { return w.f.Close() }

func decodeBatch(p []byte) ([]entry, error) {
	cnt, n := binary.Uvarint(p)
	if n <= 0 {
		return nil, errCorrupt
	}
	p = p[n:]
	var out []entry
	for i := uint64(0); i < cnt; i++ {
		e, n, err := decodeEntry(p)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
		p = p[n:]
	}
	return out, nil
}

// replayWAL applies every intact record. It stops silently at the first torn
// or corrupt record: that is the tail of a write interrupted by a crash.
func replayWAL(path string, apply func([]entry)) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	pos := 0
	for len(data)-pos >= walHeaderSize {
		n := int(binary.LittleEndian.Uint32(data[pos:]))
		sum := binary.LittleEndian.Uint32(data[pos+4:])
		if len(data)-pos-walHeaderSize < n {
			break
		}
		payload := data[pos+walHeaderSize : pos+walHeaderSize+n]
		if crc32.Checksum(payload, castagnoli) != sum {
			break
		}
		batch, err := decodeBatch(payload)
		if err != nil {
			break
		}
		apply(batch)
		pos += walHeaderSize + n
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
