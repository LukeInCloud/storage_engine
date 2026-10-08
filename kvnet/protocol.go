// Package kvnet is the network layer: a compact length-prefixed binary
// protocol over TCP (stdlib only), a server that exposes any Store, and a client.
//
// Frame:    len(u32 LE) | payload                     (len <= MaxFrame)
// Request:  op(1) | args
// Response: status(1) | body
//
// Strings/bytes are encoded as uvarint length + data. Requests on one
// connection are answered in order, so clients may pipeline.
package kvnet

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"kvstore/engine"
)

const (
	opPut      byte = 1 // key, value
	opRead     byte = 2 // key
	opRange    byte = 3 // flags(bit0 start, bit1 end), [start], [end]  (inclusive)
	opBatchPut byte = 4 // n, n x (key, value)
	opDelete   byte = 5 // key
)

const (
	stOK        byte = 0
	stNotFound  byte = 1
	stError     byte = 2 // body: message
	stNotLeader byte = 3 // body: leader address ("" if unknown)
)

// MaxFrame bounds a single request or response (guards against bad peers).
const MaxFrame = 64 << 20

// Store is what the server exposes; *engine.DB implements it, and so does the
// replicated store.
type Store interface {
	Put(key, value []byte) error
	Read(key []byte) ([]byte, error)
	ReadKeyRange(start, end []byte) ([]engine.KV, error)
	BatchPut(keys, values [][]byte) error
	Delete(key []byte) error
}

// NotLeaderError is returned by a replicated Store on a follower.
type NotLeaderError struct{ Leader string }

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("kvnet: not the leader (leader=%q)", e.Leader)
}

var errFrameTooLarge = errors.New("kvnet: frame too large")

func writeFrame(w *bufio.Writer, payload []byte) error {
	var h [4]byte
	binary.LittleEndian.PutUint32(h[:], uint32(len(payload)))
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readFrame(r *bufio.Reader) ([]byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(h[:])
	if n > MaxFrame {
		return nil, errFrameTooLarge
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

// dec is a sticky-error decoder.
type dec struct {
	b   []byte
	err error
}

var errMalformed = errors.New("kvnet: malformed message")

func (d *dec) byte() byte {
	if d.err != nil || len(d.b) < 1 {
		d.err = errMalformed
		return 0
	}
	c := d.b[0]
	d.b = d.b[1:]
	return c
}

func (d *dec) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errMalformed
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *dec) bytes() []byte {
	n := d.uvarint()
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.b)) {
		d.err = errMalformed
		return nil
	}
	v := d.b[:n:n]
	d.b = d.b[n:]
	return v
}

func appendUvarint(dst []byte, v uint64) []byte { return binary.AppendUvarint(dst, v) }
