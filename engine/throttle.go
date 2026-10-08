package engine

import (
	"sync"
	"time"
)

// throttle is an adaptive write-rate limiter. Compaction has a finite
// throughput; if writers outrun it, level 0 piles up and, without a brake, the
// engine would hit a hard stop and every writer would freeze for the length of
// a big merge. Instead the limiter lowers the accepted rate *before* that point
// (multiplicative decrease while level 0 keeps growing, gradual recovery when
// it drains), so latency stays smooth and the rate settles near what
// compaction can sustain.
type throttle struct {
	mu   sync.Mutex
	rate float64   // bytes per second; 0 = unlimited
	next time.Time // earliest time the next byte may be admitted
}

const (
	minThrottleRate = 256 << 10 // never throttle below 256 KiB/s
	maxThrottleWait = 200 * time.Millisecond
	throttleTick    = 100 * time.Millisecond
)

// wait blocks the writer so that admitted bytes follow the current rate.
func (t *throttle) wait(n int) time.Duration {
	t.mu.Lock()
	if t.rate == 0 {
		t.mu.Unlock()
		return 0
	}
	now := time.Now()
	if t.next.Before(now) {
		t.next = now
	}
	d := t.next.Sub(now)
	t.next = t.next.Add(time.Duration(float64(n) / t.rate * float64(time.Second)))
	t.mu.Unlock()
	if d > maxThrottleWait {
		d = maxThrottleWait
	}
	if d > 0 {
		time.Sleep(d)
	}
	return d
}

func (t *throttle) set(rate float64) {
	t.mu.Lock()
	t.rate = rate
	t.mu.Unlock()
}

func (t *throttle) get() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rate
}

// throttleLoop adjusts the rate every tick from the state of level 0.
func (db *DB) throttleLoop() {
	defer db.bgWG.Done()
	tick := time.NewTicker(throttleTick)
	defer tick.Stop()
	var lastBytes, lastL0 int64
	for {
		select {
		case <-db.quit:
			return
		case <-tick.C:
		}
		if !db.compactionEnabled() {
			continue
		}
		n := db.opts.CompactionTrigger
		db.mu.RLock()
		l0 := int64(len(db.cur.levels[0]))
		db.mu.RUnlock()
		written := db.ctr.writtenBytes.Load()
		ingest := float64(written-lastBytes) / throttleTick.Seconds()
		lastBytes = written

		rate := db.thr.get()
		switch {
		case l0 >= int64(2*n):
			if rate == 0 { // engage just below the rate we were being fed at
				rate = ingest * 0.9
			} else if l0 >= lastL0 {
				rate *= 0.85 // not draining: back off
			}
			if rate < minThrottleRate {
				rate = minThrottleRate
			}
		case l0 < int64(n) && rate != 0:
			rate *= 1.25 // compaction caught up: recover gradually
			if rate > 4*ingest && rate > 64<<20 {
				rate = 0 // far above any demand: lift the limit
			}
		}
		lastL0 = l0
		db.thr.set(rate)
	}
}
