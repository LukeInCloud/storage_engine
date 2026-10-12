"""Small synchronisation and I/O helpers shared by all packages."""

import os
import threading


class VarintError(ValueError):
    pass


def append_uvarint(dst: bytearray, v: int) -> None:
    while v >= 0x80:
        dst.append((v & 0x7F) | 0x80)
        v >>= 7
    dst.append(v)


def uvarint_bytes(v: int) -> bytes:
    b = bytearray()
    append_uvarint(b, v)
    return bytes(b)


def read_uvarint(b, pos: int = 0):
    """Decode a uvarint at b[pos:]; returns (value, new_pos). Raises VarintError."""
    shift = 0
    result = 0
    n = len(b)
    while pos < n:
        c = b[pos]
        pos += 1
        result |= (c & 0x7F) << shift
        if c < 0x80:
            if result >= 1 << 64:
                raise VarintError("varint overflows 64 bits")
            return result, pos
        shift += 7
        if shift > 63:
            raise VarintError("varint too long")
    raise VarintError("truncated varint")


class AtomicInt:
    def __init__(self, value: int = 0):
        self._lock = threading.Lock()
        self._v = value

    def add(self, n: int) -> int:
        with self._lock:
            self._v += n
            return self._v

    def load(self) -> int:
        with self._lock:
            return self._v

    def store(self, v: int) -> None:
        with self._lock:
            self._v = v


class Signal:
    """A coalescing wake-up flag (a buffered channel of capacity one).

    kick() never blocks; wait() returns True when kicked and False once the
    signal was closed and no kick is pending.
    """

    def __init__(self):
        self._cv = threading.Condition(threading.Lock())
        self._set = False
        self._closed = False

    def kick(self) -> None:
        with self._cv:
            self._set = True
            self._cv.notify()

    def wait(self) -> bool:
        with self._cv:
            while not self._set and not self._closed:
                self._cv.wait()
            if self._set:
                self._set = False
                return True
            return False

    def close(self) -> None:
        with self._cv:
            self._closed = True
            self._cv.notify_all()


class _Held:
    def __init__(self, acquire, release):
        self._acquire, self._release = acquire, release

    def __enter__(self):
        self._acquire()

    def __exit__(self, *exc):
        self._release()


class RWLock:
    """Writer-preferring readers/writer lock (not reentrant)."""

    def __init__(self):
        self._cv = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer = False
        self._writers_waiting = 0

    def acquire_read(self) -> None:
        with self._cv:
            while self._writer or self._writers_waiting:
                self._cv.wait()
            self._readers += 1

    def release_read(self) -> None:
        with self._cv:
            self._readers -= 1
            if self._readers == 0:
                self._cv.notify_all()

    def acquire_write(self) -> None:
        with self._cv:
            self._writers_waiting += 1
            while self._writer or self._readers:
                self._cv.wait()
            self._writers_waiting -= 1
            self._writer = True

    def release_write(self) -> None:
        with self._cv:
            self._writer = False
            self._cv.notify_all()

    def read(self):
        return _Held(self.acquire_read, self.release_read)

    def write(self):
        return _Held(self.acquire_write, self.release_write)


def sync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_all(fd: int, data) -> None:
    view = memoryview(data)
    while len(view):
        n = os.write(fd, view)
        view = view[n:]


def pwrite_all(fd: int, data, offset: int) -> None:
    view = memoryview(data)
    while len(view):
        n = os.pwrite(fd, view, offset)
        offset += n
        view = view[n:]


def pread_exact(fd: int, n: int, offset: int) -> bytes:
    """Read exactly n bytes at offset (raises EOFError when the file is shorter)."""
    out = bytearray()
    while len(out) < n:
        chunk = os.pread(fd, n - len(out), offset + len(out))
        if not chunk:
            raise EOFError("unexpected end of file")
        out += chunk
    return bytes(out)


def write_file_atomic(path: str, data: bytes, directory: str) -> None:
    """Write temp file, fsync, rename over path, fsync the directory."""
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        write_all(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.rename(tmp, path)
    sync_dir(directory)


class WaitGroup:
    """Counts running threads; wait() blocks until all have called done()."""

    def __init__(self):
        self._cv = threading.Condition(threading.Lock())
        self._n = 0

    def add(self, n: int = 1) -> None:
        with self._cv:
            self._n += n

    def done(self) -> None:
        with self._cv:
            self._n -= 1
            if self._n <= 0:
                self._cv.notify_all()

    def wait(self) -> None:
        with self._cv:
            while self._n > 0:
                self._cv.wait()

    def go(self, fn, *args, name: str = None) -> threading.Thread:
        """Run fn(*args) in a tracked daemon thread."""
        self.add()

        def run():
            try:
                fn(*args)
            finally:
                self.done()

        t = threading.Thread(target=run, name=name, daemon=True)
        t.start()
        return t


_DURATION_UNITS = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_duration(s: str) -> float:
    """Parse "1.5s", "250ms", "2m", "1h30m" (or a bare number of seconds) into seconds."""
    import re

    s = s.strip()
    try:
        return float(s)
    except ValueError:
        pass
    parts = re.findall(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)", s)
    if not parts or "".join(a + b for a, b in parts) != s:
        raise ValueError("invalid duration %r" % s)
    return sum(float(n) * _DURATION_UNITS[u] for n, u in parts)
