import threading
import time

MIN_THROTTLE_RATE = 256 << 10  # never throttle below 256 KiB/s
MAX_THROTTLE_WAIT = 0.2  # seconds
THROTTLE_TICK = 0.1  # seconds


class Throttle:
    """Adaptive write-rate limiter. Compaction has a finite throughput; if
    writers outrun it, level 0 piles up and, without a brake, the engine would
    hit a hard stop and every writer would freeze for the length of a big
    merge. Instead the limiter lowers the accepted rate *before* that point
    (multiplicative decrease while level 0 keeps growing, gradual recovery when
    it drains), so latency stays smooth and the rate settles near what
    compaction can sustain."""

    def __init__(self):
        self._lock = threading.Lock()
        self._rate = 0.0  # bytes per second; 0 = unlimited
        self._next = 0.0  # earliest (monotonic) time the next byte may be admitted

    def wait(self, n: int) -> float:
        """Block the writer so that admitted bytes follow the current rate;
        returns the time slept in seconds."""
        with self._lock:
            if self._rate == 0:
                return 0.0
            now = time.monotonic()
            if self._next < now:
                self._next = now
            d = self._next - now
            self._next += n / self._rate
        if d > MAX_THROTTLE_WAIT:
            d = MAX_THROTTLE_WAIT
        if d > 0:
            time.sleep(d)
        return d

    def set(self, rate: float) -> None:
        with self._lock:
            self._rate = rate

    def get(self) -> float:
        with self._lock:
            return self._rate


class ThrottleMixin:
    def _throttle_loop(self) -> None:
        """Adjust the rate every tick from the state of level 0."""
        last_bytes = 0
        last_l0 = 0
        while not self.quit.wait(THROTTLE_TICK):
            if not self._compaction_enabled():
                continue
            n = self.opts.compaction_trigger
            with self.mu:
                l0 = len(self.cur.levels[0])
            written = self.ctr.get("written_bytes")
            ingest = (written - last_bytes) / THROTTLE_TICK
            last_bytes = written

            rate = self.thr.get()
            if l0 >= 2 * n:
                if rate == 0:  # engage just below the rate we were being fed at
                    rate = ingest * 0.9
                elif l0 >= last_l0:
                    rate *= 0.85  # not draining: back off
                if rate < MIN_THROTTLE_RATE:
                    rate = MIN_THROTTLE_RATE
            elif l0 < n and rate != 0:
                rate *= 1.25  # compaction caught up: recover gradually
                if rate > 4 * ingest and rate > 64 << 20:
                    rate = 0  # far above any demand: lift the limit
            last_l0 = l0
            self.thr.set(rate)
