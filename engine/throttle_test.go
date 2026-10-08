package engine

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestThrottleSpacesAdmissionsAtTheRate(t *testing.T) {
	var th throttle
	if d := th.wait(1 << 20); d != 0 {
		t.Fatalf("unlimited throttle slept %v", d)
	}
	th.set(100 << 10) // 100 KiB/s
	start := time.Now()
	for i := 0; i < 4; i++ {
		th.wait(10 << 10) // 10 KiB each => 100ms apart; the first is admitted at once
	}
	if el := time.Since(start); el < 250*time.Millisecond || el > 600*time.Millisecond {
		t.Fatalf("4 admissions at 100KiB/s took %v, want ~300ms", el)
	}
	th.set(0)
	start = time.Now()
	th.wait(1 << 30)
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("limit not lifted")
	}
}

func TestThrottleWaitIsBounded(t *testing.T) {
	var th throttle
	th.set(minThrottleRate)
	th.wait(100 << 20) // would be minutes at this rate
	t0 := time.Now()
	th.wait(1)
	if d := time.Since(t0); d > maxThrottleWait+100*time.Millisecond {
		t.Fatalf("a single wait lasted %v, cap is %v", d, maxThrottleWait)
	}
}

func TestStatsReflectActivity(t *testing.T) {
	db := openOpts(t, t.TempDir(), levelOpts())
	defer db.Close()
	for i := 0; i < 3000; i++ {
		db.Put([]byte(fmt.Sprintf("k%05d", i)), make([]byte, 40))
	}
	waitFor(t, "compactions", func() bool { return db.Stats().Compactions+db.Stats().TrivialMoves > 0 })
	s := db.Stats()
	if s.Flushes == 0 || s.FlushBytes == 0 {
		t.Fatalf("no flushes recorded: %+v", s)
	}
	files := 0
	for _, l := range s.Levels {
		files += l.Files
	}
	if n, _ := liveTables(db); files != n {
		t.Fatalf("stats list %d files, version has %d", files, n)
	}
	if !strings.Contains(s.String(), "levels:") {
		t.Fatal("String() lacks the level summary")
	}
}
