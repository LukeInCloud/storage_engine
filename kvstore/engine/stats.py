import threading
from dataclasses import dataclass, field

from .manifest import NUM_LEVELS


class Counters:
    """Counters updated by the write path and the background workers."""

    NAMES = (
        "written_bytes", "flushes", "flush_bytes", "compactions", "trivial_moves",
        "compact_bytes_read", "compact_bytes_write", "compact_nanos",
        "stalls", "stall_nanos", "slowdowns", "slowdown_nanos",
    )

    def __init__(self):
        self._lock = threading.Lock()
        self._v = dict.fromkeys(self.NAMES, 0)

    def add(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._v[name] += n

    def get(self, name: str) -> int:
        with self._lock:
            return self._v[name]


@dataclass
class LevelStats:
    files: int = 0
    bytes: int = 0


@dataclass
class Stats:
    """A point-in-time view of the engine, for monitoring and tuning."""

    levels: list = field(default_factory=lambda: [LevelStats() for _ in range(NUM_LEVELS)])
    flushes: int = 0
    flush_bytes: int = 0
    compactions: int = 0
    trivial_moves: int = 0
    compaction_bytes_read: int = 0
    compaction_bytes_written: int = 0
    compaction_time: float = 0.0  # seconds
    # Write throttling caused by compaction lagging behind ingestion.
    slowdowns: int = 0
    slowdown_time: float = 0.0
    stalls: int = 0
    stall_time: float = 0.0
    memtable_bytes: int = 0
    throttle_rate: float = 0.0  # admitted write rate in bytes/s (0 = no limit)

    def __str__(self) -> str:
        parts = ["levels:"]
        for l, ls in enumerate(self.levels):
            if ls.files > 0:
                parts.append(" L%d=%d files/%dMB" % (l, ls.files, ls.bytes >> 20))
        if self.throttle_rate > 0:
            parts.append(" throttle=%.1fMB/s" % (self.throttle_rate / (1 << 20)))
        parts.append(
            " | flushes=%d (%dMB) compactions=%d (+%d moves) read=%dMB wrote=%dMB busy=%.0fms"
            " | slowdowns=%d (%.0fms) stalls=%d (%.0fms)"
            % (self.flushes, self.flush_bytes >> 20, self.compactions, self.trivial_moves,
               self.compaction_bytes_read >> 20, self.compaction_bytes_written >> 20,
               self.compaction_time * 1000, self.slowdowns, self.slowdown_time * 1000,
               self.stalls, self.stall_time * 1000))
        return "".join(parts)
