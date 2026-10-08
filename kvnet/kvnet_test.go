package kvnet

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"kvstore/engine"
)

func start(t *testing.T, dir string) (*engine.DB, *Server) {
	t.Helper()
	db, err := engine.Open(dir, &engine.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := Listen("127.0.0.1:0", db)
	if err != nil {
		t.Fatal(err)
	}
	return db, srv
}

func dial(t *testing.T, srv *Server) *Client {
	t.Helper()
	c, err := Dial(srv.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestAllOperations(t *testing.T) {
	db, srv := start(t, t.TempDir())
	defer db.Close()
	defer srv.Close()
	c := dial(t, srv)

	if _, err := c.Read([]byte("a")); err != engine.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if err := c.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Read([]byte("a")); err != nil || string(v) != "1" {
		t.Fatalf("Read = %q, %v", v, err)
	}
	if err := c.BatchPut([][]byte{[]byte("b"), []byte("c"), []byte("d")}, [][]byte{[]byte("2"), []byte("3"), []byte("4")}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete([]byte("c")); err != nil {
		t.Fatal(err)
	}
	kvs, err := c.ReadKeyRange([]byte("a"), []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	var got string
	for _, kv := range kvs {
		got += string(kv.Key) + "=" + string(kv.Value) + " "
	}
	if got != "a=1 b=2 d=4 " {
		t.Fatalf("range = %q", got)
	}
	all, _ := c.ReadKeyRange(nil, nil)
	if len(all) != 3 {
		t.Fatalf("open range returned %d", len(all))
	}
	if err := c.Put(nil, []byte("x")); err == nil {
		t.Fatal("empty key must be rejected")
	}
	// Empty value is legal and must round-trip.
	c.Put([]byte("e"), nil)
	if v, err := c.Read([]byte("e")); err != nil || len(v) != 0 {
		t.Fatalf("empty value: %q %v", v, err)
	}
}

func TestLargeValuesAndRange(t *testing.T) {
	db, srv := start(t, t.TempDir())
	defer db.Close()
	defer srv.Close()
	c := dial(t, srv)
	big := bytes.Repeat([]byte("x"), 5<<20)
	if err := c.Put([]byte("big"), big); err != nil {
		t.Fatal(err)
	}
	v, err := c.Read([]byte("big"))
	if err != nil || !bytes.Equal(v, big) {
		t.Fatalf("big value mismatch (%v)", err)
	}
	kvs, err := c.ReadKeyRange(nil, nil)
	if err != nil || len(kvs) != 1 || len(kvs[0].Value) != len(big) {
		t.Fatalf("range with big value: %v", err)
	}
}

func TestConcurrentClients(t *testing.T) {
	db, srv := start(t, t.TempDir())
	defer db.Close()
	defer srv.Close()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c, err := Dial(srv.Addr(), 5*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			for i := 0; i < 500; i++ {
				k := []byte(fmt.Sprintf("w%d-%04d", w, i))
				if err := c.Put(k, k); err != nil {
					t.Error(err)
					return
				}
				if v, err := c.Read(k); err != nil || !bytes.Equal(v, k) {
					t.Errorf("read back %s: %q %v", k, v, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	c := dial(t, srv)
	kvs, _ := c.ReadKeyRange([]byte("w3-"), []byte("w3-\xff"))
	if len(kvs) != 500 {
		t.Fatalf("w3 has %d keys", len(kvs))
	}
}

func TestDataSurvivesServerRestart(t *testing.T) {
	dir := t.TempDir()
	db, srv := start(t, dir)
	c := dial(t, srv)
	if err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	srv.Close()
	db.Close()

	db, srv = start(t, dir)
	defer db.Close()
	defer srv.Close()
	c2 := dial(t, srv)
	if v, err := c2.Read([]byte("k")); err != nil || string(v) != "v" {
		t.Fatalf("after restart: %q %v", v, err)
	}
}

func TestClientReconnectsAfterConnectionLoss(t *testing.T) {
	db, srv := start(t, t.TempDir())
	defer db.Close()
	defer srv.Close()
	c := dial(t, srv)
	c.Put([]byte("a"), []byte("1"))
	c.mu.Lock()
	c.conn.Close() // simulate network drop
	c.mu.Unlock()
	if err := c.Put([]byte("b"), []byte("2")); err == nil {
		t.Fatal("expected error on dead connection")
	}
	if v, err := c.Read([]byte("a")); err != nil || string(v) != "1" {
		t.Fatalf("after reconnect: %q %v", v, err)
	}
}

func TestServerSurvivesGarbage(t *testing.T) {
	db, srv := start(t, t.TempDir())
	defer db.Close()
	defer srv.Close()

	send := func(b []byte) {
		conn, err := net.Dial("tcp", srv.Addr())
		if err != nil {
			t.Fatal(err)
		}
		conn.Write(b)
		conn.SetReadDeadline(time.Now().Add(time.Second))
		buf := make([]byte, 64)
		conn.Read(buf)
		conn.Close()
	}
	send([]byte{0xff, 0xff, 0xff, 0xff, 1, 2, 3}) // absurd frame length
	send([]byte{3, 0, 0, 0, opPut, 0xff, 0xff})   // truncated args
	send([]byte{2, 0, 0, 0, opBatchPut, 0xff})    // bad count
	send([]byte{1, 0, 0, 0, 99})                  // unknown op
	send([]byte{10, 0, 0, 0, opRead})             // short frame then EOF
	c := dial(t, srv)
	if err := c.Put([]byte("still"), []byte("alive")); err != nil {
		t.Fatalf("server broken by garbage: %v", err)
	}
}

func TestServerCloseDisconnectsClients(t *testing.T) {
	db, srv := start(t, t.TempDir())
	defer db.Close()
	c := dial(t, srv)
	c.Put([]byte("a"), []byte("1"))
	srv.Close() // must not hang on the open connection
	if err := c.Put([]byte("b"), []byte("2")); err == nil {
		t.Fatal("expected error after server close")
	}
}
