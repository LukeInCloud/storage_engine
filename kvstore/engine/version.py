import bisect
import threading

from .manifest import NUM_LEVELS, Manifest

# The tables of a DB are organised in levels (leveled compaction, the rolling
# merge of the LSM-tree paper):
#
#   - level 0 holds freshly flushed memtables; files may overlap each other and
#     are ordered oldest -> newest;
#   - levels 1.. hold sorted, NON-overlapping files; every level is ~10x larger
#     than the previous one, and shallower levels always hold newer data.
#
# A compaction merges a few files of level L with the files of level L+1 that
# overlap them, so its cost is bounded (a few tens of MiB) no matter how large
# the database is. This keeps write stalls short and latency predictable.


class Version:
    """An immutable snapshot of the table layout. Readers pin one with a single
    reference count instead of touching every table; a version holds a
    reference on each of its tables, which keeps their files alive until the
    last reader of any version containing them is done."""

    def __init__(self, levels):
        self.levels = levels  # list of NUM_LEVELS lists of SSTable
        self._refs = 1
        self._lock = threading.Lock()
        for files in levels:
            for t in files:
                t.ref()

    def ref(self) -> None:
        with self._lock:
            self._refs += 1

    def unref(self) -> None:
        with self._lock:
            self._refs -= 1
            last = self._refs == 0
        if last:
            for files in self.levels:
                for t in files:
                    t.unref()

    def level_bytes(self, level: int) -> int:
        return sum(t.size for t in self.levels[level])

    def manifest(self, log_num: int) -> Manifest:
        m = Manifest(log_num=log_num)
        for level, files in enumerate(self.levels):
            m.levels[level] = [t.id for t in files]
        return m

    def with_changes(self, remove, add):
        """The level layout with `remove` (a set of tables) taken out and `add`
        (per-level lists) inserted. Level 0 keeps its order (new files
        appended); other levels stay sorted by key."""
        out = []
        for level in range(NUM_LEVELS):
            files = [t for t in self.levels[level] if not remove or t not in remove]
            if add is not None:
                files.extend(add[level])
            if level > 0:
                files.sort(key=lambda t: t.smallest)
            out.append(files)
        return out

    def get(self, key: bytes):
        """The newest entry for key stored in the tables, or None."""
        l0 = self.levels[0]
        for i in range(len(l0) - 1, -1, -1):
            t = l0[i]
            if t.covers(key):
                e = t.get(key)
                if e is not None:
                    return e
        for level in range(1, NUM_LEVELS):
            files = self.levels[level]
            i = bisect.bisect_left(files, key, key=lambda t: t.largest)
            if i < len(files) and files[i].covers(key):
                e = files[i].get(key)
                if e is not None:
                    return e
        return None

    def iterators(self, start, fill: bool):
        """One iterator per level-0 file (newest first) and one per deeper
        level, in order of decreasing recency, positioned at start."""
        its = []
        l0 = self.levels[0]
        for i in range(len(l0) - 1, -1, -1):
            if start is None or l0[i].largest >= start:
                its.append(l0[i].iter(start, fill))
        for level in range(1, NUM_LEVELS):
            if self.levels[level]:
                its.append(level_iter(self.levels[level], start, fill))
        return its


def overlapping(files, lo: bytes, hi: bytes):
    """The files of a sorted, disjoint level that intersect [lo, hi]."""
    i = bisect.bisect_left(files, lo, key=lambda t: t.largest)
    j = i
    while j < len(files) and files[j].smallest <= hi:
        j += 1
    return files[i:j]


def level_iter(files, start, fill: bool):
    """Walk the files of one sorted level as a single stream, opening each
    file only when the scan reaches it."""
    idx = 0
    if start is not None:
        idx = bisect.bisect_left(files, start, key=lambda t: t.largest)
    s = start
    for i in range(idx, len(files)):
        yield from files[i].iter(s, fill)
        s = None
