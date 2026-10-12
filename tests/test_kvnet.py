import socket
import threading
import time
import unittest

from kvstore import kvnet
from kvstore.engine import DB, NotFoundError, Options
from kvstore.kvnet import protocol
from tests.helpers import TempDirTestCase


class KvnetTestCase(TempDirTestCase):
    def start(self, directory):
        db = DB.open(directory, Options(no_sync=True))
        srv = kvnet.listen("127.0.0.1:0", db)
        return db, srv

    def dial(self, srv):
        c = kvnet.dial(srv.addr(), 5)
        self.addCleanup(c.close)
        return c


class TestKvnet(KvnetTestCase):
    def test_all_operations(self):
        db, srv = self.start(self.tmpdir())
        self.addCleanup(db.close)
        self.addCleanup(srv.close)
        c = self.dial(srv)
        with self.assertRaises(NotFoundError):
            c.read(b"a")
        c.put(b"a", b"1")
        self.assertEqual(c.read(b"a"), b"1")
        c.batch_put([b"b", b"c", b"d"], [b"2", b"3", b"4"])
        c.delete(b"c")
        kvs = c.read_key_range(b"a", b"d")
        self.assertEqual([(kv.key, kv.value) for kv in kvs], [(b"a", b"1"), (b"b", b"2"), (b"d", b"4")])
        self.assertEqual(len(c.read_key_range(None, None)), 3)
        with self.assertRaises(Exception):
            c.put(b"", b"x")  # empty key must be rejected
        c.put(b"e", b"")  # an empty value is legal and must round-trip
        self.assertEqual(c.read(b"e"), b"")

    def test_large_values_and_range(self):
        db, srv = self.start(self.tmpdir())
        self.addCleanup(db.close)
        self.addCleanup(srv.close)
        c = self.dial(srv)
        big = b"x" * (5 << 20)
        c.put(b"big", big)
        self.assertEqual(c.read(b"big"), big)
        kvs = c.read_key_range(None, None)
        self.assertEqual(len(kvs), 1)
        self.assertEqual(len(kvs[0].value), len(big))

    def test_concurrent_clients(self):
        db, srv = self.start(self.tmpdir())
        self.addCleanup(db.close)
        self.addCleanup(srv.close)
        errors = []

        def run(w):
            try:
                c = kvnet.dial(srv.addr(), 5)
                try:
                    for i in range(200):
                        k = b"w%d-%04d" % (w, i)
                        c.put(k, k)
                        self.assertEqual(c.read(k), k)
                finally:
                    c.close()
            except Exception as e:  # noqa
                errors.append(e)

        ts = [threading.Thread(target=run, args=(w,)) for w in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errors, [])
        c = self.dial(srv)
        self.assertEqual(len(c.read_key_range(b"w3-", b"w3-\xff")), 200)

    def test_data_survives_server_restart(self):
        d = self.tmpdir()
        db, srv = self.start(d)
        c = self.dial(srv)
        c.put(b"k", b"v")
        srv.close()
        db.close()
        db, srv = self.start(d)
        self.addCleanup(db.close)
        self.addCleanup(srv.close)
        self.assertEqual(self.dial(srv).read(b"k"), b"v")

    def test_client_reconnects_after_connection_loss(self):
        db, srv = self.start(self.tmpdir())
        self.addCleanup(db.close)
        self.addCleanup(srv.close)
        c = self.dial(srv)
        c.put(b"a", b"1")
        with c.mu:
            c.conn.close()  # simulate a network drop
        with self.assertRaises(Exception):
            c.put(b"b", b"2")
        self.assertEqual(c.read(b"a"), b"1")  # reconnected on the next call

    def test_server_survives_garbage(self):
        db, srv = self.start(self.tmpdir())
        self.addCleanup(db.close)
        self.addCleanup(srv.close)

        def send(b: bytes):
            host, port = srv.addr().rsplit(":", 1)
            with socket.create_connection((host, int(port)), timeout=2) as s:
                s.sendall(b)
                s.settimeout(1)
                try:
                    s.recv(64)
                except OSError:
                    pass

        send(b"\xff\xff\xff\xff\x01\x02\x03")  # absurd frame length
        send(bytes([3, 0, 0, 0, protocol.OP_PUT, 0xFF, 0xFF]))  # truncated args
        send(bytes([2, 0, 0, 0, protocol.OP_BATCH_PUT, 0xFF]))  # bad count
        send(bytes([1, 0, 0, 0, 99]))  # unknown op
        send(bytes([10, 0, 0, 0, protocol.OP_READ]))  # short frame then EOF
        self.dial(srv).put(b"still", b"alive")

    def test_server_close_disconnects_clients(self):
        db, srv = self.start(self.tmpdir())
        self.addCleanup(db.close)
        c = self.dial(srv)
        c.put(b"a", b"1")
        t0 = time.monotonic()
        srv.close()  # must not hang on the open connection
        self.assertLess(time.monotonic() - t0, 3)
        with self.assertRaises(Exception):
            c.put(b"b", b"2")


if __name__ == "__main__":
    unittest.main()
