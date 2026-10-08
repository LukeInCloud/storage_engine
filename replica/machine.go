// Package replica connects the Raft node to the storage engine: a state
// machine that applies replicated commands to an engine.DB, and a
// kvnet.Store that routes writes through consensus and reads through ReadIndex.
package replica

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"kvstore/engine"
	"kvstore/raft"
)

// Reserved key holding (index, term) of the last applied command. It is
// written in the same atomic batch as the command's effects, so the data and
// the applied index can never disagree after a crash.
var (
	reservedPrefix = []byte("\x00raft/")
	appliedKey     = []byte("\x00raft/applied")
)

func isReserved(key []byte) bool { return bytes.HasPrefix(key, reservedPrefix) }

// ---- command encoding ----

// EncodeCommand serialises a batch of operations as one replicated command.
func EncodeCommand(ops []engine.Op) []byte {
	b := binary.AppendUvarint(nil, uint64(len(ops)))
	for _, o := range ops {
		kind := byte(1)
		if o.Delete {
			kind = 0
		}
		b = append(b, kind)
		b = binary.AppendUvarint(b, uint64(len(o.Key)))
		b = append(b, o.Key...)
		b = binary.AppendUvarint(b, uint64(len(o.Value)))
		b = append(b, o.Value...)
	}
	return b
}

var errBadCommand = errors.New("replica: malformed command")

func decodeCommand(b []byte) ([]engine.Op, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > uint64(len(b)) {
		return nil, errBadCommand
	}
	b = b[k:]
	ops := make([]engine.Op, 0, n)
	for i := uint64(0); i < n; i++ {
		if len(b) < 1 {
			return nil, errBadCommand
		}
		kind := b[0]
		b = b[1:]
		var parts [2][]byte
		for j := range parts {
			l, k := binary.Uvarint(b)
			if k <= 0 || l > uint64(len(b)-k) {
				return nil, errBadCommand
			}
			parts[j] = b[k : k+int(l)]
			b = b[k+int(l):]
		}
		ops = append(ops, engine.Op{Key: parts[0], Value: parts[1], Delete: kind == 0})
	}
	return ops, nil
}

// ---- state machine ----

// KVMachine implements raft.StateMachine on top of an engine.DB.
//
// Layout under dir:  db/ (live data), db.restore/ (snapshot being installed),
// restore.ready (commit point of a snapshot install).
type KVMachine struct {
	dir  string
	opts engine.Options

	mu      sync.RWMutex // guards db (swapped by snapshot installs)
	db      *engine.DB
	staging *engine.DB
}

func OpenKVMachine(dir string, opts *engine.Options) (*KVMachine, error) {
	m := &KVMachine{dir: dir}
	if opts != nil {
		m.opts = *opts
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// Finish or roll back an interrupted snapshot install.
	ready, staged, live := filepath.Join(dir, "restore.ready"), filepath.Join(dir, "db.restore"), filepath.Join(dir, "db")
	if _, err := os.Stat(ready); err == nil {
		if _, err := os.Stat(staged); err == nil {
			if err := os.RemoveAll(live); err != nil {
				return nil, err
			}
			if err := os.Rename(staged, live); err != nil {
				return nil, err
			}
		}
		if err := os.Remove(ready); err != nil {
			return nil, err
		}
	} else if err := os.RemoveAll(staged); err != nil {
		return nil, err
	}
	db, err := engine.Open(live, &m.opts)
	if err != nil {
		return nil, err
	}
	m.db = db
	return m, nil
}

func (m *KVMachine) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.staging != nil {
		m.staging.Close()
		m.staging = nil
	}
	return m.db.Close()
}

func encodeApplied(index, term uint64) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:], index)
	binary.LittleEndian.PutUint64(b[8:], term)
	return b
}

func (m *KVMachine) Applied() (uint64, uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, err := m.db.Read(appliedKey)
	if err == engine.ErrNotFound {
		return 0, 0, nil
	}
	if err != nil || len(v) != 16 {
		return 0, 0, errors.New("replica: bad applied marker")
	}
	return binary.LittleEndian.Uint64(v[0:]), binary.LittleEndian.Uint64(v[8:]), nil
}

func (m *KVMachine) Apply(index, term uint64, cmds [][]byte) error {
	var ops []engine.Op
	for _, c := range cmds {
		o, err := decodeCommand(c)
		if err != nil {
			return err
		}
		ops = append(ops, o...)
	}
	ops = append(ops, engine.Op{Key: appliedKey, Value: encodeApplied(index, term)})
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db.Write(ops)
}

// SnapshotScan pages through user data (the reserved marker is skipped).
func (m *KVMachine) SnapshotScan(cursor []byte, maxBytes int) ([]raft.KV, []byte, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	kvs, more, err := m.db.Scan(cursor, maxBytes)
	if err != nil {
		return nil, nil, false, err
	}
	var items []raft.KV
	for _, kv := range kvs {
		if !isReserved(kv.Key) {
			items = append(items, raft.KV{Key: kv.Key, Value: kv.Value})
		}
	}
	var next []byte
	if more {
		next = append(append([]byte{}, kvs[len(kvs)-1].Key...), 0)
	}
	return items, next, !more, nil
}

func (m *KVMachine) RestoreBegin() error {
	m.RestoreAbort()
	staged := filepath.Join(m.dir, "db.restore")
	if err := os.RemoveAll(staged); err != nil {
		return err
	}
	// Default options: every chunk is fsynced, so "ready" implies durable.
	db, err := engine.Open(staged, &engine.Options{BlockCacheSize: -1, CompactionTrigger: -1})
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.staging = db
	m.mu.Unlock()
	return nil
}

func (m *KVMachine) RestoreChunk(items []raft.KV) error {
	if len(items) == 0 {
		return nil
	}
	ops := make([]engine.Op, len(items))
	for i, kv := range items {
		ops[i] = engine.Op{Key: kv.Key, Value: kv.Value}
	}
	m.mu.RLock()
	st := m.staging
	m.mu.RUnlock()
	if st == nil {
		return errors.New("replica: no restore in progress")
	}
	return st.Write(ops)
}

func (m *KVMachine) RestoreFinish(index, term uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.staging == nil {
		return errors.New("replica: no restore in progress")
	}
	if err := m.staging.Write([]engine.Op{{Key: appliedKey, Value: encodeApplied(index, term)}}); err != nil {
		return err
	}
	if err := m.staging.Close(); err != nil {
		return err
	}
	m.staging = nil

	// Commit point: once restore.ready exists, OpenKVMachine completes the swap.
	ready := filepath.Join(m.dir, "restore.ready")
	f, err := os.Create(ready)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := syncDir(m.dir); err != nil {
		return err
	}
	if err := m.db.Close(); err != nil {
		return err
	}
	live, staged := filepath.Join(m.dir, "db"), filepath.Join(m.dir, "db.restore")
	if err := os.RemoveAll(live); err != nil {
		return err
	}
	if err := os.Rename(staged, live); err != nil {
		return err
	}
	if err := os.Remove(ready); err != nil {
		return err
	}
	if err := syncDir(m.dir); err != nil {
		return err
	}
	db, err := engine.Open(live, &m.opts)
	if err != nil {
		return err
	}
	m.db = db
	return nil
}

func (m *KVMachine) RestoreAbort() {
	m.mu.Lock()
	st := m.staging
	m.staging = nil
	m.mu.Unlock()
	if st != nil {
		st.Close()
		os.RemoveAll(filepath.Join(m.dir, "db.restore"))
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ---- reads (callers must have passed raft.Node.ReadBarrier) ----

func (m *KVMachine) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db.Read(key)
}

func (m *KVMachine) Range(start, end []byte) ([]engine.KV, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	kvs, err := m.db.ReadKeyRange(start, end)
	if err != nil {
		return nil, err
	}
	out := kvs[:0]
	for _, kv := range kvs {
		if !isReserved(kv.Key) {
			out = append(out, kv)
		}
	}
	return out, nil
}
