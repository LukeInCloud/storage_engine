"""kvbench drives a kvserver (standalone or cluster) and reports throughput and
latency percentiles, once per second and in total.

    python3 -m kvstore.cmd.kvbench -mode fill -n 50000 -conc 16 -value 100     random-order writes of keys 0..n-1
    python3 -m kvstore.cmd.kvbench -mode read -n 50000 -conc 16 -duration 10s  random reads of keys written by fill
    python3 -m kvstore.cmd.kvbench -mode scan -n 50000 -scan 100 -duration 10s short range scans
    python3 -m kvstore.cmd.kvbench -mode mixed -n 50000 -conc 16 -duration 10s 50% reads / 50% writes

Keys are 16 hex digits of a hash of the index, so insertion order is random.
CPython threads share one interpreter lock: run several kvbench processes
(against one server) to generate more load than one process can.
"""

import argparse
import math
import random
import sys
import threading
import time

from .. import kvnet
from ..engine import NotFoundError
from ..util import parse_duration

BUCKETS = 256
MASK64 = (1 << 64) - 1


class Hist:
    """Log-scale latency histogram (8 buckets per octave of microseconds)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.c = [0] * BUCKETS
        self.n = 0

    def add(self, seconds: float) -> None:
        us = seconds * 1e6 + 1
        b = min(int(math.log2(us) * 8), BUCKETS - 1)
        with self.lock:
            self.c[b] += 1
            self.n += 1

    def snapshot(self):
        with self.lock:
            return list(self.c), self.n


def bucket_us(b: int) -> float:
    return 2 ** (b / 8)


def percentile(c, n: int, p: float) -> float:
    """Latency (seconds) at percentile p."""
    if n == 0:
        return 0.0
    target = math.ceil(n * p)
    seen = 0
    for b in range(BUCKETS):
        seen += c[b]
        if seen >= target:
            return bucket_us(b + 1) / 1e6
    return 0.0


def fmt(sec: float) -> str:
    if sec >= 1:
        return "%.2fs" % sec
    if sec >= 1e-3:
        return "%.2fms" % (sec * 1e3)
    return "%.0fµs" % (sec * 1e6)


def mix64(x: int) -> int:  # splitmix64
    x = (x + 0x9E3779B97F4A7C15) & MASK64
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & MASK64
    return x ^ (x >> 31)


def key_of(i: int) -> bytes:
    return b"%016x" % mix64(i)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="kvbench")
    ap.add_argument("-addr", "--addr", default="127.0.0.1:7000", help="server address (comma separated list = cluster)")
    ap.add_argument("-mode", "--mode", default="fill", choices=["fill", "read", "scan", "mixed"])
    ap.add_argument("-n", "--n", type=int, default=100000, help="number of distinct keys (fill: operations)")
    ap.add_argument("-conc", "--conc", type=int, default=32, help="concurrent connections")
    ap.add_argument("-value", "--value", type=int, default=100, help="value size in bytes")
    ap.add_argument("-batch", "--batch", type=int, default=1, help="keys per write request (fill/mixed)")
    ap.add_argument("-scan", "--scan", type=int, default=100, help="approximate keys per range scan")
    ap.add_argument("-duration", "--duration", default="10s", help="run time for read/scan/mixed")
    args = ap.parse_args(argv)
    duration = parse_duration(args.duration)

    h = Hist()
    lock = threading.Lock()
    total = [0]
    errs = [0]
    next_idx = [0]  # fill: next index to write
    start = time.monotonic()
    deadline = start + duration
    value = bytes(ord("a") + i % 26 for i in range(args.value))

    def new_client():
        try:
            if "," in args.addr:
                return kvnet.dial_cluster(args.addr.split(","), 30.0, 30.0)
            return kvnet.dial(args.addr, 30.0)
        except OSError as e:
            print(e, file=sys.stderr)
            sys.exit(1)

    class Done(Exception):
        pass

    def op(c, rnd):
        mode = args.mode
        if mode == "fill":
            with lock:
                i = next_idx[0]
                next_idx[0] += args.batch
            if i >= args.n:
                raise Done()
            if args.batch == 1:
                c.put(key_of(i), value)
                return 1
            ks = [key_of(i + j) for j in range(args.batch) if i + j < args.n]
            c.batch_put(ks, [value] * len(ks))
            return len(ks)
        if mode == "read":
            try:
                c.read(key_of(rnd.randrange(args.n)))
            except NotFoundError:
                pass
            return 1
        if mode == "scan":
            s = rnd.getrandbits(64)
            width = (MASK64 // args.n) * args.scan
            kvs = c.read_key_range(b"%016x" % s, b"%016x" % ((s + width) & MASK64))
            return len(kvs)
        if mode == "mixed":
            if rnd.randrange(2) == 0:
                try:
                    c.read(key_of(rnd.randrange(args.n)))
                except NotFoundError:
                    pass
            else:
                c.put(key_of(rnd.randrange(args.n)), value)
            return 1
        raise ValueError("unknown mode %r" % mode)

    def worker(w):
        c = new_client()
        rnd = random.Random(w + time.time_ns())
        try:
            while args.mode == "fill" or time.monotonic() < deadline:
                t0 = time.monotonic()
                try:
                    items = op(c, rnd)
                except Done:
                    return
                except Exception as e:
                    with lock:
                        errs[0] += 1
                        if errs[0] <= 5:
                            print("error: %s" % e, file=sys.stderr)
                    continue
                h.add(time.monotonic() - t0)
                with lock:
                    total[0] += items
        finally:
            c.close()

    threads = [threading.Thread(target=worker, args=(w,), daemon=True) for w in range(args.conc)]
    for t in threads:
        t.start()

    print("%6s %12s %9s %9s %9s" % ("t", "items/s", "p50", "p99", "max"))
    prev, prev_n, prev_items = [0] * BUCKETS, 0, 0
    next_tick = start + 1.0
    while any(t.is_alive() for t in threads):
        time.sleep(0.02)
        if time.monotonic() < next_tick:
            continue
        next_tick += 1.0
        c, cn = h.snapshot()
        d = [c[i] - prev[i] for i in range(BUCKETS)]
        with lock:
            items = total[0]
        max_b = max((i for i in range(BUCKETS) if d[i] > 0), default=0)
        print("%5.0fs %12d %9s %9s %9s" % (
            time.monotonic() - start, items - prev_items, fmt(percentile(d, cn - prev_n, 0.50)),
            fmt(percentile(d, cn - prev_n, 0.99)), fmt(bucket_us(max_b + 1) / 1e6)))
        prev, prev_n, prev_items = c, cn, items
    for t in threads:
        t.join()
    el = time.monotonic() - start
    c, cn = h.snapshot()
    print("\n%s: %d items, %d requests, %d errors in %.1fs  =>  %.0f items/s" % (
        args.mode, total[0], cn, errs[0], el, total[0] / el))
    print("request latency  p50=%s p95=%s p99=%s p99.9=%s" % (
        fmt(percentile(c, cn, 0.50)), fmt(percentile(c, cn, 0.95)),
        fmt(percentile(c, cn, 0.99)), fmt(percentile(c, cn, 0.999))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
