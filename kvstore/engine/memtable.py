import bisect
import threading

from .codec import Entry

NODE_OVERHEAD = 48


class Memtable:
    """In-memory sorted table holding only the newest version of every key.

    A hash map answers point lookups; a sorted key array (bisect) serves
    ordered scans. In CPython this is much faster than a pointer-chasing skip
    list because the searching and shifting happen in C.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._map = {}
        self._keys = []
        self.size = 0
        self.count = 0

    def approx_size(self) -> int:
        return self.size

    def __len__(self) -> int:
        return self.count

    def put(self, key: bytes, value: bytes, kind: int) -> None:
        with self._lock:
            old = self._map.get(key)
            if old is not None:
                self.size += len(value) - len(old.value)
                self._map[key] = Entry(key, value, kind)
                return
            bisect.insort(self._keys, key)
            self._map[key] = Entry(key, value, kind)
            self.size += len(key) + len(value) + NODE_OVERHEAD
            self.count += 1

    def get(self, key: bytes):
        return self._map.get(key)

    def iter(self, start=None, end=None):
        """Iterate entries with start <= key <= end in key order. The iterator
        works on a snapshot taken now (nil bounds are open)."""
        with self._lock:
            keys = self._keys
            i = 0 if start is None else bisect.bisect_left(keys, start)
            j = len(keys) if end is None else bisect.bisect_right(keys, end)
            m = self._map
            snap = [m[k] for k in keys[i:j]]
        return iter(snap)
