package engine

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// counters are updated lock-free by the write path and the background workers.
type counters struct {
	writtenBytes                        atomic.Int64
	flushes, flushBytes                 atomic.Int64
	compactions, trivialMoves           atomic.Int64
	compactBytesRead, compactBytesWrite atomic.Int64
	compactNanos                        atomic.Int64
	stalls, stallNanos                  atomic.Int64
	slowdowns, slowdownNanos            atomic.Int64
}

type LevelStats struct {
	Files int
	Bytes int64
}

// Stats is a point-in-time view of the engine, for monitoring and tuning.
type Stats struct {
	Levels [numLevels]LevelStats

	Flushes, FlushBytes int64

	Compactions, TrivialMoves int64
	CompactionBytesRead       int64
	CompactionBytesWritten    int64
	CompactionTime            time.Duration

	// Write throttling caused by compaction lagging behind ingestion.
	Slowdowns    int64
	SlowdownTime time.Duration
	Stalls       int64
	StallTime    time.Duration

	MemtableBytes int
	// ThrottleRate is the current admitted write rate in bytes/s (0 = no limit).
	ThrottleRate float64
}

func (db *DB) Stats() Stats {
	var s Stats
	db.mu.RLock()
	for l, files := range db.cur.levels {
		s.Levels[l].Files = len(files)
		s.Levels[l].Bytes = db.cur.levelBytes(l)
	}
	s.MemtableBytes = db.mem.approxSize()
	db.mu.RUnlock()
	s.ThrottleRate = db.thr.get()
	c := &db.ctr
	s.Flushes, s.FlushBytes = c.flushes.Load(), c.flushBytes.Load()
	s.Compactions, s.TrivialMoves = c.compactions.Load(), c.trivialMoves.Load()
	s.CompactionBytesRead, s.CompactionBytesWritten = c.compactBytesRead.Load(), c.compactBytesWrite.Load()
	s.CompactionTime = time.Duration(c.compactNanos.Load())
	s.Slowdowns, s.SlowdownTime = c.slowdowns.Load(), time.Duration(c.slowdownNanos.Load())
	s.Stalls, s.StallTime = c.stalls.Load(), time.Duration(c.stallNanos.Load())
	return s
}

func (s Stats) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "levels:")
	for l, ls := range s.Levels {
		if ls.Files > 0 {
			fmt.Fprintf(&b, " L%d=%d files/%dMB", l, ls.Files, ls.Bytes>>20)
		}
	}
	if s.ThrottleRate > 0 {
		fmt.Fprintf(&b, " throttle=%.1fMB/s", s.ThrottleRate/(1<<20))
	}
	fmt.Fprintf(&b, " | flushes=%d (%dMB) compactions=%d (+%d moves) read=%dMB wrote=%dMB busy=%v | slowdowns=%d (%v) stalls=%d (%v)",
		s.Flushes, s.FlushBytes>>20, s.Compactions, s.TrivialMoves, s.CompactionBytesRead>>20, s.CompactionBytesWritten>>20,
		s.CompactionTime.Round(time.Millisecond), s.Slowdowns, s.SlowdownTime.Round(time.Millisecond), s.Stalls, s.StallTime.Round(time.Millisecond))
	return b.String()
}
