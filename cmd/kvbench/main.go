// kvbench drives a kvserver (standalone or cluster) and reports throughput and
// latency percentiles, once per second and in total.
//
//	kvbench -mode fill -n 500000 -conc 64 -value 100        random-order writes of keys 0..n-1
//	kvbench -mode read -n 500000 -conc 32 -duration 10s     random reads of keys written by fill
//	kvbench -mode scan -n 500000 -scan 100 -duration 10s    short range scans
//	kvbench -mode mixed -n 500000 -conc 32 -duration 10s    50% reads / 50% writes
//
// Keys are 16 hex digits of a hash of the index, so insertion order is random.
package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kvstore/kvnet"
)

const buckets = 256

// hist is a lock-free log-scale latency histogram (8 buckets per octave of microseconds).
type hist struct {
	c [buckets]atomic.Int64
	n atomic.Int64
}

func (h *hist) add(d time.Duration) {
	us := float64(d.Microseconds()) + 1
	b := int(math.Log2(us) * 8)
	if b >= buckets {
		b = buckets - 1
	}
	h.c[b].Add(1)
	h.n.Add(1)
}

func bucketUs(b int) float64 { return math.Pow(2, float64(b)/8) }

func (h *hist) snapshot() (c [buckets]int64, n int64) {
	for i := range c {
		c[i] = h.c[i].Load()
	}
	return c, h.n.Load()
}

func percentile(c [buckets]int64, n int64, p float64) time.Duration {
	if n == 0 {
		return 0
	}
	target := int64(math.Ceil(float64(n) * p))
	var seen int64
	for b := 0; b < buckets; b++ {
		seen += c[b]
		if seen >= target {
			return time.Duration(bucketUs(b+1)) * time.Microsecond
		}
	}
	return 0
}

func mix64(x uint64) uint64 { // splitmix64
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func keyOf(i uint64) []byte { return []byte(fmt.Sprintf("%016x", mix64(i))) }

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "server address (comma separated list = cluster)")
	mode := flag.String("mode", "fill", "fill | read | scan | mixed")
	n := flag.Uint64("n", 100000, "number of distinct keys (fill: operations)")
	conc := flag.Int("conc", 32, "concurrent connections")
	vsize := flag.Int("value", 100, "value size in bytes")
	batch := flag.Int("batch", 1, "keys per write request (fill/mixed)")
	scan := flag.Int("scan", 100, "approximate keys per range scan")
	dur := flag.Duration("duration", 10*time.Second, "run time for read/scan/mixed")
	flag.Parse()

	var total, h = new(atomic.Int64), &hist{}
	var errs atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(*dur)
	var next atomic.Uint64 // fill: next index to write

	newClient := func() *kvnet.Client {
		var c *kvnet.Client
		var err error
		if strings.Contains(*addr, ",") {
			c, err = kvnet.DialCluster(strings.Split(*addr, ","), 10*time.Second, 30*time.Second)
		} else {
			c, err = kvnet.Dial(*addr, 30*time.Second)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return c
	}
	value := make([]byte, *vsize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	op := func(c *kvnet.Client, rnd *rand.Rand) (items int, err error) {
		switch *mode {
		case "fill":
			i := next.Add(uint64(*batch)) - uint64(*batch)
			if i >= *n {
				return 0, errDone
			}
			if *batch == 1 {
				return 1, c.Put(keyOf(i), value)
			}
			var ks, vs [][]byte
			for j := uint64(0); j < uint64(*batch) && i+j < *n; j++ {
				ks, vs = append(ks, keyOf(i+j)), append(vs, value)
			}
			return len(ks), c.BatchPut(ks, vs)
		case "read":
			_, err := c.Read(keyOf(uint64(rnd.Int63n(int64(*n)))))
			return 1, err
		case "scan":
			s := rnd.Uint64()
			width := math.MaxUint64 / *n * uint64(*scan)
			kvs, err := c.ReadKeyRange([]byte(fmt.Sprintf("%016x", s)), []byte(fmt.Sprintf("%016x", s+width)))
			return len(kvs), err
		case "mixed":
			if rnd.Intn(2) == 0 {
				_, err := c.Read(keyOf(uint64(rnd.Int63n(int64(*n)))))
				return 1, err
			}
			return 1, c.Put(keyOf(uint64(rnd.Int63n(int64(*n)))), value)
		}
		return 0, fmt.Errorf("unknown mode %q", *mode)
	}

	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := newClient()
			defer c.Close()
			rnd := rand.New(rand.NewSource(int64(w) + time.Now().UnixNano()))
			for *mode == "fill" || time.Now().Before(deadline) {
				t0 := time.Now()
				items, err := op(c, rnd)
				if err == errDone {
					return
				}
				if err != nil {
					if errs.Add(1) <= 5 {
						fmt.Fprintf(os.Stderr, "error: %v\n", err)
					}
					continue
				}
				h.add(time.Since(t0))
				total.Add(int64(items))
			}
		}(w)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	fmt.Printf("%6s %12s %9s %9s %9s\n", "t", "items/s", "p50", "p99", "max")
	var prev [buckets]int64
	var prevN, prevItems int64
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	report := func() {
		c, cn := h.snapshot()
		var d [buckets]int64
		for i := range d {
			d[i] = c[i] - prev[i]
		}
		items := total.Load()
		var maxB int
		for i := range d {
			if d[i] > 0 {
				maxB = i
			}
		}
		fmt.Printf("%5.0fs %12d %9s %9s %9s\n", time.Since(start).Seconds(), items-prevItems,
			percentile(d, cn-prevN, 0.50), percentile(d, cn-prevN, 0.99), time.Duration(bucketUs(maxB+1))*time.Microsecond)
		prev, prevN, prevItems = c, cn, items
	}
loop:
	for {
		select {
		case <-tick.C:
			report()
		case <-done:
			break loop
		}
	}
	el := time.Since(start)
	c, cn := h.snapshot()
	fmt.Printf("\n%s: %d items, %d requests, %d errors in %.1fs  =>  %.0f items/s\n",
		*mode, total.Load(), cn, errs.Load(), el.Seconds(), float64(total.Load())/el.Seconds())
	fmt.Printf("request latency  p50=%v p95=%v p99=%v p99.9=%v\n",
		percentile(c, cn, 0.50), percentile(c, cn, 0.95), percentile(c, cn, 0.99), percentile(c, cn, 0.999))
}

var errDone = fmt.Errorf("done")
