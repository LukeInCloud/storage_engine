package engine

import (
	"encoding/binary"
	"errors"
)

const (
	kindDelete byte = 0
	kindPut    byte = 1
)

var errCorrupt = errors.New("engine: corrupt data")

// entry is the unit stored in every layer (WAL, memtable, SSTable).
// A delete is stored as a tombstone (kindDelete) that shadows older versions.
type entry struct {
	key, value []byte
	kind       byte
}

// appendEntry encodes: kind(1) | klen(uvarint) | vlen(uvarint) | key | value.
func appendEntry(dst []byte, e entry) []byte {
	dst = append(dst, e.kind)
	dst = binary.AppendUvarint(dst, uint64(len(e.key)))
	dst = binary.AppendUvarint(dst, uint64(len(e.value)))
	dst = append(dst, e.key...)
	return append(dst, e.value...)
}

// decodeEntry decodes one entry; returned slices alias b.
func decodeEntry(b []byte) (entry, int, error) {
	if len(b) < 1 || b[0] > kindPut {
		return entry{}, 0, errCorrupt
	}
	kind := b[0]
	pos := 1
	kl, n := binary.Uvarint(b[pos:])
	if n <= 0 {
		return entry{}, 0, errCorrupt
	}
	pos += n
	vl, n := binary.Uvarint(b[pos:])
	if n <= 0 {
		return entry{}, 0, errCorrupt
	}
	pos += n
	rest := uint64(len(b) - pos)
	if kl > rest || vl > rest-kl {
		return entry{}, 0, errCorrupt
	}
	key := b[pos : pos+int(kl)]
	pos += int(kl)
	val := b[pos : pos+int(vl)]
	pos += int(vl)
	return entry{key: key, value: val, kind: kind}, pos, nil
}

// iterator is implemented by memtable, SSTable and merge iterators.
type iterator interface {
	Valid() bool
	Entry() entry
	Next()
	Err() error
}
