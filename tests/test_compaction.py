import glob
import os
import random
import threading
import time
import unittest

from kvstore.engine import DB, Options
from kvstore.engine.cache import BlockCache
from kvstore.engine.codec import KIND_DELETE
from kvstore.engine.sstable import sst_path
from kvstore.engine.throttle import MAX_THROTTLE_WAIT, MIN_THROTTLE_RATE, Throttle
from tests.helpers import TempDirTestCase, wait_for


def level_opts(**kw) -> Options:
    """Small sizes so that data cascades through several levels in a short test."""
    d = dict(memtable_size=4 << 10, compaction_trigger=3, target_file_size=8 << 10,
             level_base_bytes=16 << 10, no_sync=True)
    d.update(kw)
    return Options(**d)


def live_tables(db):
    with db.mu:
        ids = {t.id for files in db.cur.levels for t in files}
    return len(ids), ids


def raw_entries(db):
    """Every stored entry (live, overwritten and tombstones) in all tables."""
    total = tombs = 0
    with db.mu:
        for files in db.cur.levels:
            for tb in files:
                for e in tb.iter(None, False):
                    total += 1
                    tombs += e.kind == KIND_DELETE
    return total, tombs


class TestCompaction(TempDirTestCase):
    def test_compaction_preserves_data(self):
        """Compaction must never change what reads return while readers and
        writers run concurrently; invariants must hold and data must cascade."""
        d = self.tmpdir()
        o = level_opts()
        db = DB.open(d, o)
        model = {}
        stop = threading.Event()
        errors = []

        def reader():
            try:
                while not stop.is_set():
                    db.read_key_range(None, None)
                    try:
                        db.read(b"key-00007")
                    except Exception as e:
                        if type(e).__name__ != "NotFoundError":
                            raise
            except Exception as e:  # noqa
                errors.append(e)

        readers = [threading.Thread(target=reader) for _ in range(2)]
        for t in readers:
            t.start()
        rnd = random.Random(7)
        for i in range(8000):
            k = b"key-%05d" % rnd.randrange(1500)
            if rnd.randrange(4) == 0:
                db.delete(k)
                model.pop(k, None)
            else:
                v = b"v%d" % i
                db.put(k, v)
                model[k] = v
            if i % 1000 == 0:
                db.check_levels()
        stop.set()
        for t in readers:
            t.join()
        self.assertEqual(errors, [])

        def verify(db):
            got = db.read_key_range(None, None)
            self.assertEqual({kv.key: kv.value for kv in got}, model)
            self.assertEqual(len(got), len(model))
            for k, v in model.items():
                self.assertEqual(db.read(k), v)
            db.check_levels()

        verify(db)
        wait_for(lambda: db.level_files()[0] < o.compaction_trigger, 10, "level 0 to drain")
        verify(db)
        self.assertGreater(db.level_files()[2], 0, "data never cascaded below level 1: %s" % db.level_files())
        db.close()
        db = DB.open(d, o)
        self.addCleanup(db.close)
        verify(db)

    def test_compact_all_drops_tombstones_and_old_versions(self):
        db = DB.open(self.tmpdir(), level_opts())
        self.addCleanup(db.close)
        for rnd in range(5):  # many overwritten versions per key
            for i in range(300):
                db.put(b"k%04d" % i, b"round%d" % rnd)
        for i in range(0, 300, 2):
            db.delete(b"k%04d" % i)
        db.compact_all()
        db.check_levels()
        total, tombs = raw_entries(db)
        self.assertEqual(tombs, 0)
        self.assertLessEqual(total, 150)
        got = db.read_key_range(None, None)
        self.assertEqual(len(got), 150)
        for kv in got:
            self.assertEqual(kv.value, b"round4")

    def test_trivial_moves_keep_data_intact(self):
        """Sequential keys never overlap, so files move down without rewriting."""
        db = DB.open(self.tmpdir(), level_opts())
        self.addCleanup(db.close)
        for i in range(3000):
            db.put(b"seq-%06d" % i, b"payload-payload-payload")
        wait_for(lambda: db.level_files()[0] < 3, 10, "compaction")
        db.check_levels()
        self.assertEqual(len(db.read_key_range(None, None)), 3000)

    def test_obsolete_files_deleted_after_last_reader(self):
        d = self.tmpdir()
        # A huge trigger: nothing compacts on its own, only the explicit compact_all.
        db = DB.open(d, Options(memtable_size=2 << 10, compaction_trigger=1000,
                                target_file_size=8 << 10, no_sync=True))
        self.addCleanup(db.close)
        for i in range(1500):
            db.put(b"k%05d" % (i % 400), bytes(30))
        wait_for(lambda: db.level_files()[0] >= 4, 10, "flushes")

        snap = db.snapshot()  # a reader that pins the current layout
        _, pinned = live_tables(db)
        db.compact_all()
        for fid in pinned:
            self.assertTrue(os.path.exists(sst_path(d, fid)), "table %06d deleted while pinned" % fid)
        snap.v.get(b"k00001")  # the pinned reader still works
        snap.release()
        _, live = live_tables(db)
        wait_for(lambda: len(glob.glob(os.path.join(d, "*.sst"))) == len(live), 5,
                 "obsolete files to disappear")

    def test_group_commit_durable_and_concurrent(self):
        d = self.tmpdir()
        db = DB.open(d, Options())  # fsync on
        errors = []

        def w(n):
            try:
                for i in range(50):
                    k = b"w%02d-%03d" % (n, i)
                    db.put(k, k)
            except Exception as e:  # noqa
                errors.append(e)

        ts = [threading.Thread(target=w, args=(n,)) for n in range(16)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errors, [])
        db.close()
        db = DB.open(d, Options())
        self.addCleanup(db.close)
        for n in range(16):
            for i in range(50):
                k = b"w%02d-%03d" % (n, i)
                self.assertEqual(db.read(k), k)

    def test_group_commit_no_starvation(self):
        """A group-commit leader must not be kept busy serving others while its
        own caller waits: every write completes in bounded time."""
        db = DB.open(self.tmpdir(), Options())  # fsync on
        self.addCleanup(db.close)
        stop = time.monotonic() + 1.5
        worst = [0.0]
        lock = threading.Lock()
        errors = []

        def w(n):
            try:
                i = 0
                while time.monotonic() < stop:
                    t0 = time.monotonic()
                    db.put(b"w%02d-%06d" % (n, i), b"v")
                    d = time.monotonic() - t0
                    with lock:
                        worst[0] = max(worst[0], d)
                    i += 1
            except Exception as e:  # noqa
                errors.append(e)

        ts = [threading.Thread(target=w, args=(n,)) for n in range(24)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errors, [])
        self.assertLess(worst[0], 0.75, "a write took %.3fs under load: leader starvation" % worst[0])


class TestCacheAndThrottle(TempDirTestCase):
    def test_block_cache_lru(self):
        c = BlockCache(100)
        c.put((1, 1), object(), 40)
        c.put((1, 2), object(), 40)
        c.get((1, 1))  # 1 is now most recent
        c.put((1, 3), object(), 40)
        self.assertIsNone(c.get((1, 2)), "LRU victim should have been evicted")
        self.assertIsNotNone(c.get((1, 1)))
        c.put((1, 9), object(), 1000)  # larger than the cache: never stored
        self.assertIsNone(c.get((1, 9)))

    def test_reads_hit_cache(self):
        d = self.tmpdir()
        db = DB.open(d, Options(memtable_size=2 << 10, compaction_trigger=-1, no_sync=True))
        self.addCleanup(db.close)
        for i in range(400):
            db.put(b"k%04d" % i, bytes(40))
        wait_for(lambda: len(glob.glob(os.path.join(d, "*.sst"))) >= 3, 5, "flush")
        for i in range(400):
            db.read(b"k%04d" % i)
        self.assertGreater(db.cache.used, 0)

    def test_throttle_spaces_admissions_at_the_rate(self):
        th = Throttle()
        self.assertEqual(th.wait(1 << 20), 0)
        th.set(100 << 10)  # 100 KiB/s
        start = time.monotonic()
        for _ in range(4):
            th.wait(10 << 10)  # 10 KiB each => 100 ms apart; the first is admitted at once
        el = time.monotonic() - start
        self.assertTrue(0.25 < el < 0.6, "4 admissions took %.3fs, want ~0.3" % el)
        th.set(0)
        start = time.monotonic()
        th.wait(1 << 30)
        self.assertLess(time.monotonic() - start, 0.05)

    def test_throttle_wait_is_bounded(self):
        th = Throttle()
        th.set(MIN_THROTTLE_RATE)
        th.wait(100 << 20)  # would be minutes at this rate
        t0 = time.monotonic()
        th.wait(1)
        self.assertLess(time.monotonic() - t0, MAX_THROTTLE_WAIT + 0.1)

    def test_stats_reflect_activity(self):
        db = DB.open(self.tmpdir(), level_opts())
        self.addCleanup(db.close)
        for i in range(3000):
            db.put(b"k%05d" % i, bytes(40))
        wait_for(lambda: db.stats().compactions + db.stats().trivial_moves > 0, 10, "compactions")
        s = db.stats()
        self.assertGreater(s.flushes, 0)
        self.assertGreater(s.flush_bytes, 0)
        files = sum(l.files for l in s.levels)
        # Flushes/compactions may land between the two reads, so only sanity-check.
        self.assertGreater(files, 0)
        self.assertIn("levels:", str(s))


if __name__ == "__main__":
    unittest.main()
