package engine

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The crash test re-executes this test binary as a writer process that is
// killed with SIGKILL at random moments (during WAL appends, flushes and
// compactions). After every kill the parent reopens the database and checks
// that every acknowledged operation survived.

const crashEnv = "ENGINE_CRASH_WRITER_DIR"

func TestMain(m *testing.M) {
	if dir := os.Getenv(crashEnv); dir != "" {
		crashWriter(dir)
		return
	}
	os.Exit(m.Run())
}

// op(i) is deterministic: replaying a prefix of operations always yields the
// same state, so the expected state is a pure function of "how many are acked".
func crashOp(i int) (key, value string, del bool) {
	if i%10 == 9 {
		return crashKey(i - 5), "", true
	}
	return crashKey(i), fmt.Sprintf("v%d-%s", i, strings.Repeat("x", 100+i%97)), false
}

func crashKey(i int) string { return fmt.Sprintf("key-%07d", i) }

func crashWriter(dir string) {
	start, _ := strconv.Atoi(os.Getenv("ENGINE_CRASH_START"))
	// Tiny sizes so kills also land inside multi-level compactions. fsync is on.
	db, err := Open(dir, &Options{MemtableSize: 16 << 10, CompactionTrigger: 3, TargetFileSize: 24 << 10, LevelBaseBytes: 64 << 10})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(3)
	}
	for i := start; ; i++ {
		k, v, del := crashOp(i)
		if del {
			err = db.Delete([]byte(k))
		} else {
			err = db.Put([]byte(k), []byte(v))
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "op:", err)
			os.Exit(4)
		}
		os.Stdout.WriteString(strconv.Itoa(i) + "\n") // acknowledged
	}
}

func expectedState(n int) map[string]string {
	m := map[string]string{}
	for i := 0; i < n; i++ {
		k, v, del := crashOp(i)
		if del {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	return m
}

func sameState(db *DB, want map[string]string) (bool, error) {
	got, err := db.ReadKeyRange(nil, nil)
	if err != nil {
		return false, err
	}
	if len(got) != len(want) {
		return false, nil
	}
	for _, kv := range got {
		if want[string(kv.Key)] != string(kv.Value) {
			return false, nil
		}
	}
	return true, nil
}

func TestKill9Loop(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	dir := t.TempDir()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	acked := 0 // operations [0, acked) are acknowledged
	for round := 0; round < 25; round++ {
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(), crashEnv+"="+dir, "ENGINE_CRASH_START="+strconv.Itoa(acked))
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		lines := make(chan int, 1<<16)
		go func() {
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				if n, err := strconv.Atoi(sc.Text()); err == nil {
					lines <- n
				}
			}
			close(lines)
		}()
		time.Sleep(time.Duration(80+rnd.Intn(350)) * time.Millisecond)
		cmd.Process.Kill() // SIGKILL: no chance to flush or clean up
		cmd.Wait()
		for n := range lines {
			if n+1 > acked {
				acked = n + 1
			}
		}

		db, err := Open(dir, &Options{NoSync: true, CompactionTrigger: -1})
		if err != nil {
			t.Fatalf("round %d: reopen after crash failed: %v", round, err)
		}
		// The op that was in flight at kill time may or may not have been applied.
		ok, err := sameState(db, expectedState(acked))
		if err == nil && !ok {
			ok, err = sameState(db, expectedState(acked+1))
		}
		db.Close()
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if !ok {
			t.Fatalf("round %d: state after crash matches neither %d nor %d acknowledged ops", round, acked, acked+1)
		}
	}
	if acked < 500 {
		t.Fatalf("writer made too little progress (%d ops) to be meaningful", acked)
	}
	t.Logf("survived 25 kill -9 rounds; %d acknowledged operations verified", acked)
}
