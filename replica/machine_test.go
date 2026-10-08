package replica

import (
	"os"
	"path/filepath"
	"testing"

	"kvstore/engine"
	"kvstore/raft"
)

func openMachine(t *testing.T, dir string) *KVMachine {
	t.Helper()
	m, err := OpenKVMachine(dir, &engine.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestCommandRoundTrip(t *testing.T) {
	ops := []engine.Op{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Delete: true},
		{Key: []byte("c")}, // empty value
	}
	got, err := decodeCommand(EncodeCommand(ops))
	if err != nil || len(got) != 3 {
		t.Fatalf("decode: %v %v", got, err)
	}
	if string(got[0].Key) != "a" || string(got[0].Value) != "1" || !got[1].Delete || got[2].Delete {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	for _, bad := range [][]byte{nil, {0xff}, {5, 1}, EncodeCommand(ops)[:6]} {
		if _, err := decodeCommand(bad); err == nil {
			t.Fatalf("decode accepted garbage %v", bad)
		}
	}
}

func TestAppliedMarkerIsAtomicWithData(t *testing.T) {
	dir := t.TempDir()
	m := openMachine(t, dir)
	if i, _, _ := m.Applied(); i != 0 {
		t.Fatalf("fresh applied = %d", i)
	}
	cmd := EncodeCommand([]engine.Op{{Key: []byte("k"), Value: []byte("v")}})
	if err := m.Apply(7, 3, [][]byte{cmd}); err != nil {
		t.Fatal(err)
	}
	m.Close()
	m = openMachine(t, dir)
	defer m.Close()
	if i, tm, _ := m.Applied(); i != 7 || tm != 3 {
		t.Fatalf("applied = %d/%d", i, tm)
	}
	if v, _ := m.Get([]byte("k")); string(v) != "v" {
		t.Fatal("data lost")
	}
	if kvs, _ := m.Range(nil, nil); len(kvs) != 1 {
		t.Fatalf("internal marker leaked into range: %v", kvs)
	}
	items, _, done, _ := m.SnapshotScan(nil, 1<<20)
	if !done || len(items) != 1 {
		t.Fatalf("snapshot must exclude the marker: %v", items)
	}
}

func TestRestoreSwapsStateAtomically(t *testing.T) {
	dir := t.TempDir()
	m := openMachine(t, dir)
	defer m.Close()
	m.Apply(5, 1, [][]byte{EncodeCommand([]engine.Op{{Key: []byte("old"), Value: []byte("x")}})})

	if err := m.RestoreBegin(); err != nil {
		t.Fatal(err)
	}
	m.RestoreChunk([]raft.KV{{Key: []byte("new1"), Value: []byte("a")}})
	m.RestoreChunk([]raft.KV{{Key: []byte("new2"), Value: []byte("b")}})
	if v, _ := m.Get([]byte("old")); string(v) != "x" {
		t.Fatal("live state must be untouched until finish")
	}
	if err := m.RestoreFinish(42, 9); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Get([]byte("old")); err != engine.ErrNotFound {
		t.Fatal("old data must be gone after restore")
	}
	if v, _ := m.Get([]byte("new2")); string(v) != "b" {
		t.Fatal("restored data missing")
	}
	if i, tm, _ := m.Applied(); i != 42 || tm != 9 {
		t.Fatalf("applied after restore = %d/%d", i, tm)
	}
}

func TestAbortedRestoreLeavesStateIntact(t *testing.T) {
	dir := t.TempDir()
	m := openMachine(t, dir)
	m.Apply(1, 1, [][]byte{EncodeCommand([]engine.Op{{Key: []byte("keep"), Value: []byte("1")}})})
	m.RestoreBegin()
	m.RestoreChunk([]raft.KV{{Key: []byte("junk"), Value: []byte("1")}})
	m.RestoreAbort()
	m.Close()
	m = openMachine(t, dir)
	defer m.Close()
	if _, err := m.Get([]byte("junk")); err != engine.ErrNotFound {
		t.Fatal("aborted restore leaked data")
	}
	if v, _ := m.Get([]byte("keep")); string(v) != "1" {
		t.Fatal("live data damaged")
	}
	if _, err := os.Stat(filepath.Join(dir, "db.restore")); !os.IsNotExist(err) {
		t.Fatal("staging dir not cleaned")
	}
}

// Crash right after the commit point: the next open must complete the swap.
func TestInterruptedRestoreIsCompletedOnOpen(t *testing.T) {
	dir := t.TempDir()
	m := openMachine(t, dir)
	m.Apply(1, 1, [][]byte{EncodeCommand([]engine.Op{{Key: []byte("old"), Value: []byte("1")}})})
	m.Close()

	// Build a finished staging db by hand and drop the ready flag, as a crash would leave it.
	st, err := engine.Open(filepath.Join(dir, "db.restore"), &engine.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	st.Write([]engine.Op{{Key: []byte("fresh"), Value: []byte("1")}, {Key: appliedKey, Value: encodeApplied(99, 4)}})
	st.Close()
	os.WriteFile(filepath.Join(dir, "restore.ready"), nil, 0o644)

	m = openMachine(t, dir)
	defer m.Close()
	if v, _ := m.Get([]byte("fresh")); string(v) != "1" {
		t.Fatal("swap was not completed")
	}
	if _, err := m.Get([]byte("old")); err != engine.ErrNotFound {
		t.Fatal("old state survived the swap")
	}
	if i, _, _ := m.Applied(); i != 99 {
		t.Fatalf("applied = %d", i)
	}
}
