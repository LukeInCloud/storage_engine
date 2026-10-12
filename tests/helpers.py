import os
import shutil
import tempfile
import time
import unittest


def short() -> bool:
    """KV_SHORT=1 skips the slow crash/chaos tests (like `go test -short`)."""
    return os.environ.get("KV_SHORT") == "1"


class TempDirTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="kvstore-test-")
        self.addCleanup(shutil.rmtree, self._tmp, True)

    def tmpdir(self, name: str = "") -> str:
        d = os.path.join(self._tmp, name) if name else tempfile.mkdtemp(dir=self._tmp)
        os.makedirs(d, exist_ok=True)
        return d


def wait_for(cond, timeout: float, what: str, interval: float = 0.01) -> None:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return
        time.sleep(interval)
    raise AssertionError("timed out waiting for %s" % what)


def read_file(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def write_file(path: str, data: bytes) -> None:
    with open(path, "wb") as f:
        f.write(data)
