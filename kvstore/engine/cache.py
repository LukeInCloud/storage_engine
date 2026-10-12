import threading
from collections import OrderedDict


class BlockCache:
    """Byte-bounded LRU of decoded SSTable blocks. File ids are never reused,
    so entries of deleted tables are never hit again and simply age out."""

    def __init__(self, capacity: int):
        self.cap = capacity
        self.used = 0
        self._lock = threading.Lock()
        self._m = OrderedDict()  # key -> (block, size), most recent last

    def get(self, key):
        with self._lock:
            item = self._m.get(key)
            if item is None:
                return None
            self._m.move_to_end(key)
            return item[0]

    def put(self, key, block, size: int) -> None:
        if size > self.cap:
            return
        with self._lock:
            if key in self._m:
                return
            self._m[key] = (block, size)
            self.used += size
            while self.used > self.cap:
                _, (_, sz) = self._m.popitem(last=False)
                self.used -= sz


def new_block_cache(capacity: int):
    """A missing cache is None; callers use cache_get/cache_put helpers."""
    if capacity <= 0:
        return None
    return BlockCache(capacity)
