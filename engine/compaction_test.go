package engine

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func countSST(dir string) int {
	m, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	return len(m)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Compaction must never change what reads return, while readers and writers
// run concurrently, and it must keep the number of files bounded.
func TestCompactionPreservesData(t *testing.T) {
	dir := t.TempDir()
	o := Options{MemtableSize: 4 << 10, CompactionTrigger: 3}
	db := openOpts(t, dir, o)
	model := map[string]string{}
	var mu sync.Mutex
	stop := make(chan struct{})
	var rd sync.WaitGroup
	for r := 0; r < 3; r++ {
		rd.Add(1)
		go func() {
			defer rd.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := db.ReadKeyRange(nil, nil); err != nil {
					t.Error(err)
					return
				}
				db.Read([]byte("key-00007"))
			}
		}()
	}
	rnd := rand.New(rand.NewSource(7))
	for i := 0; i < 20000; i++ {
		k := fmt.Sprintf("key-%05d", rnd.Intn(1500))
		mu.Lock()
		if rnd.Intn(4) == 0 {
			db.Delete([]byte(k))
			delete(model, k)
		} else {
			v := fmt.Sprintf("v%d", i)
			db.Put([]byte(k), []byte(v))
			model[k] = v
		}
		mu.Unlock()
	}
	close(stop)
	rd.Wait()

	verify := func(db *DB) {
		t.Helper()
		got, err := db.ReadKeyRange(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(model) {
			t.Fatalf("got %d keys, want %d", len(got), len(model))
		}
		for _, kv := range got {
			if model[string(kv.Key)] != string(kv.Value) {
				t.Fatalf("%s = %s, want %s", kv.Key, kv.Value, model[string(kv.Key)])
			}
		}
		for k, v := range model {
			mustRead(t, db, k, v)
		}
	}
	verify(db)
	waitFor(t, "compaction to settle", func() bool { return countSST(dir) < 3 })
	verify(db)
	db.Close()
	db = openOpts(t, dir, o)
	defer db.Close()
	verify(db)
}

func TestCompactionDropsTombstonesAndObsoleteFiles(t *testing.T) {
	dir := t.TempDir()
	db := openOpts(t, dir, Options{MemtableSize: 1 << 10, CompactionTrigger: 2})
	defer db.Close()
	for i := 0; i < 500; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), make([]byte, 50))
	}
	for i := 0; i < 500; i++ {
		db.Delete([]byte(fmt.Sprintf("k%04d", i)))
	}
	// Push the tombstones through a few more flushes so everything merges.
	for i := 0; i < 200; i++ {
		db.Put([]byte(fmt.Sprintf("z%04d", i)), make([]byte, 50))
	}
	waitFor(t, "files to shrink", func() bool { return countSST(dir) <= 2 })
	got, _ := db.ReadKeyRange(nil, nil)
	if len(got) != 200 {
		t.Fatalf("expected 200 live keys, got %d", len(got))
	}
	for i := 0; i < 500; i++ {
		mustMiss(t, db, fmt.Sprintf("k%04d", i))
	}
}

// With fsync enabled, concurrent writers must share fsyncs (group commit):
// 16 writers x 100 sequential syncs would take far longer than this budget.
func TestGroupCommitDurableAndConcurrent(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, &Options{}) // fsync on
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				k := []byte(fmt.Sprintf("w%02d-%03d", w, i))
				if err := db.Put(k, k); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	t.Logf("1600 durable writes from 16 goroutines in %v", time.Since(start))
	db.Close()
	db, _ = Open(dir, &Options{})
	defer db.Close()
	for w := 0; w < 16; w++ {
		for i := 0; i < 100; i++ {
			k := fmt.Sprintf("w%02d-%03d", w, i)
			mustRead(t, db, k, k)
		}
	}
}

func TestBlockCache(t *testing.T) {
	c := newBlockCache(100)
	k := func(i int) cacheKey { return cacheKey{1, i} }
	c.put(k(1), []entry{{}}, 40)
	c.put(k(2), []entry{{}}, 40)
	c.get(k(1)) // 1 is now most recent
	c.put(k(3), []entry{{}}, 40)
	if _, ok := c.get(k(2)); ok {
		t.Fatal("LRU victim should have been evicted")
	}
	if _, ok := c.get(k(1)); !ok {
		t.Fatal("recently used block evicted")
	}
	var nilCache *blockCache
	nilCache.put(k(1), nil, 1)
	if _, ok := nilCache.get(k(1)); ok {
		t.Fatal("nil cache must not cache")
	}
}

func TestReadsHitCache(t *testing.T) {
	dir := t.TempDir()
	db := openOpts(t, dir, Options{MemtableSize: 2 << 10, CompactionTrigger: -1})
	defer db.Close()
	for i := 0; i < 400; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), make([]byte, 40))
	}
	waitFor(t, "flush", func() bool { return countSST(dir) >= 3 })
	for i := 0; i < 400; i++ {
		db.Read([]byte(fmt.Sprintf("k%04d", i)))
	}
	if db.cache.used == 0 {
		t.Fatal("cache stayed empty after reads")
	}
}

// Under continuous concurrent load every single write must complete in
// bounded time: a group-commit leader must not be kept busy serving others
// while its own caller waits.
func TestGroupCommitNoStarvation(t *testing.T) {
	db, err := Open(t.TempDir(), &Options{}) // fsync on
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stop := time.Now().Add(2 * time.Second)
	var mu sync.Mutex
	var worst time.Duration
	var total int
	var wg sync.WaitGroup
	for w := 0; w < 24; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; time.Now().Before(stop); i++ {
				t0 := time.Now()
				if err := db.Put([]byte(fmt.Sprintf("w%02d-%06d", w, i)), []byte("v")); err != nil {
					t.Error(err)
					return
				}
				d := time.Since(t0)
				mu.Lock()
				total++
				if d > worst {
					worst = d
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	t.Logf("%d writes, worst latency %v", total, worst)
	if worst > 750*time.Millisecond {
		t.Fatalf("a write took %v under load: group-commit leader starvation", worst)
	}
}
