import glob
import os
import random
import threading
import unittest

from kvstore.engine import DB, ClosedError, EmptyKeyError, NotFoundError, Op, Options
from kvstore.engine.manifest import MANIFEST_NAME
from kvstore.engine.sstable import sst_path
from tests.helpers import TempDirTestCase, read_file, write_file


def open_db(directory, mem_size=0, **kw) -> DB:
    return DB.open(directory, Options(memtable_size=mem_size, no_sync=True, **kw))


class EngineTestCase(TempDirTestCase):
    def must_read(self, db, k: str, want: str):
        self.assertEqual(db.read(k.encode()), want.encode())

    def must_miss(self, db, k: str):
        with self.assertRaises(NotFoundError):
            db.read(k.encode())


class TestBasics(EngineTestCase):
    def test_put_read_delete(self):
        db = open_db(self.tmpdir())
        self.addCleanup(db.close)
        self.must_miss(db, "a")
        db.put(b"a", b"1")
        self.must_read(db, "a", "1")
        db.put(b"a", b"2")
        self.must_read(db, "a", "2")
        db.delete(b"a")
        self.must_miss(db, "a")
        with self.assertRaises(EmptyKeyError):
            db.put(b"", b"x")
        db.delete(b"never-existed")

    def test_batch_and_range(self):
        db = open_db(self.tmpdir())
        self.addCleanup(db.close)
        ks = [k.encode() for k in "dbaec"]
        db.batch_put(ks, [b"v" + k for k in ks])
        db.delete(b"c")
        got = db.read_key_range(b"b", b"e")
        self.assertEqual([kv.key for kv in got], [b"b", b"d", b"e"])
        self.assertEqual(len(db.read_key_range(None, None)), 4)

    def test_recover_from_wal(self):
        d = self.tmpdir()
        db = open_db(d)
        db.put(b"k1", b"v1")
        db.put(b"k2", b"v2")
        db.delete(b"k1")
        db.close()  # memtable never flushed: data lives only in the WAL
        db = open_db(d)
        self.addCleanup(db.close)
        self.must_miss(db, "k1")
        self.must_read(db, "k2", "v2")

    def test_torn_wal_tail(self):
        d = self.tmpdir()
        db = open_db(d)
        for i in range(1, 4):
            db.put(b"k%d" % i, b"value")
        db.close()
        last = sorted(glob.glob(os.path.join(d, "wal-*.log")))[-1]
        os.truncate(last, os.path.getsize(last) - 3)  # tear the last record
        db = open_db(d)
        self.addCleanup(db.close)
        self.must_read(db, "k1", "value")
        self.must_read(db, "k2", "value")
        self.must_miss(db, "k3")
        db.put(b"k4", b"x")  # still accepts writes after a torn tail
        self.must_read(db, "k4", "x")

    def test_corrupt_wal_record_stops_replay(self):
        d = self.tmpdir()
        db = open_db(d)
        db.put(b"a", b"1")
        db.put(b"b", b"2")
        db.close()
        last = sorted(glob.glob(os.path.join(d, "wal-*.log")))[-1]
        data = bytearray(read_file(last))
        data[-1] ^= 0xFF
        write_file(last, bytes(data))
        db = open_db(d)
        self.addCleanup(db.close)
        self.must_read(db, "a", "1")
        self.must_miss(db, "b")

    def test_orphan_files_cleaned_up(self):
        d = self.tmpdir()
        db = open_db(d)
        db.put(b"a", b"1")
        db.close()
        write_file(sst_path(d, 999), b"garbage")
        write_file(os.path.join(d, MANIFEST_NAME + ".tmp"), b"junk")
        db = open_db(d)
        self.addCleanup(db.close)
        self.must_read(db, "a", "1")
        self.assertFalse(os.path.exists(sst_path(d, 999)))
        self.assertFalse(os.path.exists(os.path.join(d, MANIFEST_NAME + ".tmp")))

    def test_closed_db(self):
        db = open_db(self.tmpdir())
        db.close()
        with self.assertRaises(ClosedError):
            db.put(b"a", b"b")
        with self.assertRaises(ClosedError):
            db.read(b"a")

    def test_write_mixed_batch_and_scan(self):
        db = open_db(self.tmpdir(), 2 << 10)
        self.addCleanup(db.close)
        for i in range(300):
            db.put(b"k%04d" % i, bytes(20))
        db.write([Op(b"k0001", delete=True), Op(b"new", b"v")])
        self.must_miss(db, "k0001")
        self.must_read(db, "new", "v")
        paged, cursor = [], None
        while True:
            kvs, more = db.scan(cursor, 200)
            paged += kvs
            if not more:
                break
            cursor = kvs[-1].key + b"\x00"
        self.assertEqual([kv.key for kv in paged], [kv.key for kv in db.read_key_range(None, None)])


class TestRandomAndConcurrent(EngineTestCase):
    def test_random_against_model(self):
        """Randomised comparison against a dict, with a tiny memtable so data
        spreads over many SSTables, and with restarts in between."""
        d = self.tmpdir()
        mk = lambda: open_db(d, 8 << 10, compaction_trigger=-1)
        db = mk()
        model = {}
        rnd = random.Random(42)

        def check():
            keys = sorted(model)
            got = db.read_key_range(None, None)
            self.assertEqual([(kv.key, kv.value) for kv in got], [(k, model[k]) for k in keys])
            for _ in range(200):
                k = b"key-%05d" % rnd.randrange(3000)
                if k in model:
                    self.assertEqual(db.read(k), model[k])
                else:
                    with self.assertRaises(NotFoundError):
                        db.read(k)
            lo, hi = sorted((b"key-%05d" % rnd.randrange(3000), b"key-%05d" % rnd.randrange(3000)))
            sub = db.read_key_range(lo, hi)
            self.assertEqual(len(sub), len([k for k in keys if lo <= k <= hi]))

        for rnd_i in range(4):
            for i in range(2000):
                k = b"key-%05d" % rnd.randrange(3000)
                r = rnd.randrange(10)
                if r < 2:
                    db.delete(k)
                    model.pop(k, None)
                elif r == 2:
                    ks, vs = [], []
                    for _ in range(5):
                        bk = b"key-%05d" % rnd.randrange(3000)
                        bv = b"b%d-%d" % (rnd_i, i)
                        ks.append(bk)
                        vs.append(bv)
                        model[bk] = bv  # later duplicates in the batch win
                    db.batch_put(ks, vs)
                else:
                    v = b"v%d-%d-" % (rnd_i, i) + b"x" * rnd.randrange(100)
                    db.put(k, v)
                    model[k] = v
            check()
            db.close()
            db = mk()
            check()
        db.close()
        self.assertGreaterEqual(len(glob.glob(os.path.join(d, "*.sst"))), 3)

    def test_concurrent(self):
        db = open_db(self.tmpdir(), 16 << 10)
        self.addCleanup(db.close)
        errors = []

        def writer(w):
            try:
                for i in range(500):
                    k = b"w%d-%05d" % (w, i)
                    db.put(k, k)
            except Exception as e:  # noqa
                errors.append(e)

        def reader():
            try:
                for _ in range(40):
                    db.read_key_range(b"w1-", b"w2-")
                    try:
                        db.read(b"w0-00010")
                    except NotFoundError:
                        pass
            except Exception as e:  # noqa
                errors.append(e)

        ts = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
        ts += [threading.Thread(target=reader) for _ in range(2)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errors, [])
        for w in range(4):
            k = b"w%d-%05d" % (w, 499)
            self.assertEqual(db.read(k), k)


if __name__ == "__main__":
    unittest.main()
