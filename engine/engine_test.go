package engine

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func open(t testing.TB, dir string, memSize int) *DB {
	t.Helper()
	db, err := Open(dir, &Options{MemtableSize: memSize, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func openOpts(t testing.TB, dir string, o Options) *DB {
	t.Helper()
	o.NoSync = true
	db, err := Open(dir, &o)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func mustRead(t *testing.T, db *DB, k string, want string) {
	t.Helper()
	got, err := db.Read([]byte(k))
	if err != nil || string(got) != want {
		t.Fatalf("Read(%q) = %q, %v; want %q", k, got, err, want)
	}
}

func mustMiss(t *testing.T, db *DB, k string) {
	t.Helper()
	if v, err := db.Read([]byte(k)); err != ErrNotFound {
		t.Fatalf("Read(%q) = %q, %v; want ErrNotFound", k, v, err)
	}
}

func TestPutReadDelete(t *testing.T) {
	db := open(t, t.TempDir(), 0)
	defer db.Close()
	mustMiss(t, db, "a")
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	mustRead(t, db, "a", "1")
	db.Put([]byte("a"), []byte("2"))
	mustRead(t, db, "a", "2")
	db.Delete([]byte("a"))
	mustMiss(t, db, "a")
	if db.Put(nil, []byte("x")) != ErrEmptyKey {
		t.Fatal("empty key accepted")
	}
	if err := db.Delete([]byte("never-existed")); err != nil {
		t.Fatal(err)
	}
}

func TestBatchAndRange(t *testing.T) {
	db := open(t, t.TempDir(), 0)
	defer db.Close()
	var ks, vs [][]byte
	for _, k := range []string{"d", "b", "a", "e", "c"} {
		ks = append(ks, []byte(k))
		vs = append(vs, []byte("v"+k))
	}
	if err := db.BatchPut(ks, vs); err != nil {
		t.Fatal(err)
	}
	db.Delete([]byte("c"))
	got, err := db.ReadKeyRange([]byte("b"), []byte("e"))
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, kv := range got {
		keys = append(keys, string(kv.Key))
	}
	if fmt.Sprint(keys) != "[b d e]" {
		t.Fatalf("range = %v", keys)
	}
	all, _ := db.ReadKeyRange(nil, nil)
	if len(all) != 4 {
		t.Fatalf("full range has %d keys", len(all))
	}
}

func TestRecoverFromWAL(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir, 0)
	db.Put([]byte("k1"), []byte("v1"))
	db.Put([]byte("k2"), []byte("v2"))
	db.Delete([]byte("k1"))
	db.Close() // memtable never flushed: data lives only in the WAL

	db = open(t, dir, 0)
	defer db.Close()
	mustMiss(t, db, "k1")
	mustRead(t, db, "k2", "v2")
}

func TestTornWALTail(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir, 0)
	for i := 1; i <= 3; i++ {
		db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("value"))
	}
	db.Close()

	wals, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	sort.Strings(wals)
	last := wals[len(wals)-1]
	st, _ := os.Stat(last)
	if err := os.Truncate(last, st.Size()-3); err != nil { // tear the last record
		t.Fatal(err)
	}
	db = open(t, dir, 0)
	defer db.Close()
	mustRead(t, db, "k1", "value")
	mustRead(t, db, "k2", "value")
	mustMiss(t, db, "k3")
	// The store must still accept writes after recovering from a torn tail.
	db.Put([]byte("k4"), []byte("x"))
	mustRead(t, db, "k4", "x")
}

func TestCorruptWALRecordStopsReplay(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir, 0)
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Close()
	wals, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	data, _ := os.ReadFile(wals[len(wals)-1])
	data[len(data)-1] ^= 0xff // flip a bit in the last record
	os.WriteFile(wals[len(wals)-1], data, 0o644)
	db = open(t, dir, 0)
	defer db.Close()
	mustRead(t, db, "a", "1")
	mustMiss(t, db, "b")
}

func TestOrphanFilesCleanedUp(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir, 0)
	db.Put([]byte("a"), []byte("1"))
	db.Close()
	// Leftovers of a flush / manifest update that crashed half way.
	os.WriteFile(sstPath(dir, 999), []byte("garbage"), 0o644)
	os.WriteFile(filepath.Join(dir, manifestName+".tmp"), []byte("junk"), 0o644)
	db = open(t, dir, 0)
	defer db.Close()
	mustRead(t, db, "a", "1")
	if _, err := os.Stat(sstPath(dir, 999)); !os.IsNotExist(err) {
		t.Fatal("orphan sst not removed")
	}
}

// Randomised comparison against a map, with a tiny memtable so data spreads
// over many SSTables, and with restarts in between.
func TestRandomAgainstModel(t *testing.T) {
	dir := t.TempDir()
	db := openOpts(t, dir, Options{MemtableSize: 8 << 10, CompactionTrigger: -1})
	model := map[string]string{}
	rnd := rand.New(rand.NewSource(42))

	check := func() {
		t.Helper()
		var keys []string
		for k := range model {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		got, err := db.ReadKeyRange(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(keys) {
			t.Fatalf("range returned %d keys, model has %d", len(got), len(keys))
		}
		for i, kv := range got {
			if string(kv.Key) != keys[i] || string(kv.Value) != model[keys[i]] {
				t.Fatalf("mismatch at %d: %q=%q want %q=%q", i, kv.Key, kv.Value, keys[i], model[keys[i]])
			}
		}
		for i := 0; i < 200; i++ {
			k := fmt.Sprintf("key-%05d", rnd.Intn(3000))
			if want, ok := model[k]; ok {
				mustRead(t, db, k, want)
			} else {
				mustMiss(t, db, k)
			}
		}
		lo, hi := fmt.Sprintf("key-%05d", rnd.Intn(3000)), fmt.Sprintf("key-%05d", rnd.Intn(3000))
		if lo > hi {
			lo, hi = hi, lo
		}
		sub, _ := db.ReadKeyRange([]byte(lo), []byte(hi))
		n := 0
		for _, k := range keys {
			if k >= lo && k <= hi {
				n++
			}
		}
		if len(sub) != n {
			t.Fatalf("sub-range [%s,%s] got %d want %d", lo, hi, len(sub), n)
		}
	}

	for round := 0; round < 6; round++ {
		for i := 0; i < 3000; i++ {
			k := fmt.Sprintf("key-%05d", rnd.Intn(3000))
			switch rnd.Intn(10) {
			case 0, 1:
				db.Delete([]byte(k))
				delete(model, k)
			case 2:
				var ks, vs [][]byte
				for j := 0; j < 5; j++ {
					bk := fmt.Sprintf("key-%05d", rnd.Intn(3000))
					bv := fmt.Sprintf("b%d-%d", round, i)
					ks, vs = append(ks, []byte(bk)), append(vs, []byte(bv))
					model[bk] = bv
				}
				if err := db.BatchPut(ks, vs); err != nil {
					t.Fatal(err)
				}
			default:
				v := fmt.Sprintf("v%d-%d-%s", round, i, bytes.Repeat([]byte("x"), rnd.Intn(100)))
				db.Put([]byte(k), []byte(v))
				model[k] = v
			}
		}
		check()
		db.Close()
		db = openOpts(t, dir, Options{MemtableSize: 8 << 10, CompactionTrigger: -1})
		check()
	}
	db.Close()
	if tabs, _ := filepath.Glob(filepath.Join(dir, "*.sst")); len(tabs) < 3 {
		t.Fatalf("expected several SSTables, got %d", len(tabs))
	}
}

func TestConcurrent(t *testing.T) {
	db := open(t, t.TempDir(), 16<<10)
	defer db.Close()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := []byte(fmt.Sprintf("w%d-%05d", w, i))
				if err := db.Put(k, k); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if _, err := db.ReadKeyRange([]byte("w1-"), []byte("w2-")); err != nil {
					t.Error(err)
					return
				}
				db.Read([]byte("w0-00010"))
			}
		}()
	}
	wg.Wait()
	for w := 0; w < 4; w++ {
		k := fmt.Sprintf("w%d-%05d", w, 1999)
		mustRead(t, db, k, k)
	}
}

func TestClosedDB(t *testing.T) {
	db := open(t, t.TempDir(), 0)
	db.Close()
	if err := db.Put([]byte("a"), []byte("b")); err != ErrClosed {
		t.Fatalf("got %v", err)
	}
	if _, err := db.Read([]byte("a")); err != ErrClosed {
		t.Fatalf("got %v", err)
	}
}

func BenchmarkRandomPut(b *testing.B) {
	db := open(b, b.TempDir(), 4<<20)
	defer db.Close()
	rnd := rand.New(rand.NewSource(1))
	val := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put([]byte(fmt.Sprintf("%016x", rnd.Uint64())), val)
	}
}

func TestWriteMixedBatchAndScan(t *testing.T) {
	db := open(t, t.TempDir(), 2<<10)
	defer db.Close()
	for i := 0; i < 300; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), make([]byte, 20))
	}
	err := db.Write([]Op{
		{Key: []byte("k0001"), Delete: true},
		{Key: []byte("new"), Value: []byte("v")},
	})
	if err != nil {
		t.Fatal(err)
	}
	mustMiss(t, db, "k0001")
	mustRead(t, db, "new", "v")

	// Stream everything in small pages and compare to the full range.
	var paged []KV
	cursor := []byte(nil)
	for {
		kvs, more, err := db.Scan(cursor, 200)
		if err != nil {
			t.Fatal(err)
		}
		paged = append(paged, kvs...)
		if !more {
			break
		}
		cursor = append(append([]byte{}, kvs[len(kvs)-1].Key...), 0)
	}
	all, _ := db.ReadKeyRange(nil, nil)
	if len(paged) != len(all) {
		t.Fatalf("paged %d vs full %d", len(paged), len(all))
	}
	for i := range all {
		if string(paged[i].Key) != string(all[i].Key) {
			t.Fatalf("mismatch at %d", i)
		}
	}
}
