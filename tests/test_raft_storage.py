import os
import unittest

from kvstore.raft.storage import LogStore, load_hard_state, log_path, save_hard_state
from kvstore.raft.types import Entry
from tests.helpers import TempDirTestCase


def mk_entries(frm, to, term):
    return [Entry(term, i, bytes([i % 256])) for i in range(frm, to + 1)]


class TestStorage(TempDirTestCase):
    def test_hard_state_round_trip(self):
        d = self.tmpdir()
        self.assertEqual(load_hard_state(d), (0, ""))
        save_hard_state(d, 7, "n2")
        self.assertEqual(load_hard_state(d), (7, "n2"))
        save_hard_state(d, 8, "")
        self.assertEqual(load_hard_state(d), (8, ""))

    def test_log_append_reload(self):
        d = self.tmpdir()
        l = LogStore.open(d, True)
        l.append(mk_entries(1, 5, 1))
        l.append([Entry(2, 6, None)])  # no-op entry
        with self.assertRaises(ValueError):
            l.append(mk_entries(9, 9, 2))  # gap must be rejected
        l.close()

        l = LogStore.open(d, True)
        self.addCleanup(l.close)
        self.assertEqual((l.last_index(), l.last_term()), (6, 2))
        self.assertEqual(l.term(3), 1)
        self.assertIsNone(l.entries[5].data, "no-op must reload as None")

    def test_log_truncate_compact_and_torn_tail(self):
        d = self.tmpdir()
        l = LogStore.open(d, True)
        l.append(mk_entries(1, 10, 1))
        l.truncate_from(8)
        l.append(mk_entries(8, 9, 2))
        self.assertEqual(l.term(8), 2)
        l.compact_to(4)
        self.assertEqual((l.prev_index, l.entries[0].index), (4, 5))
        self.assertIsNone(l.term(3), "compacted term must be unknown")
        l.close()

        # Tear the last record in half.
        os.truncate(log_path(d), os.path.getsize(log_path(d)) - 5)
        l = LogStore.open(d, True)
        self.addCleanup(l.close)
        self.assertEqual(l.last_index(), 8)
        l.append(mk_entries(9, 9, 3))

    def test_log_rewrite_and_reset(self):
        d = self.tmpdir()
        l = LogStore.open(d, True)
        l.append(mk_entries(1, 3000, 1))
        l.compact_to(2500)  # triggers physical rewrite
        self.assertEqual(l.dropped, 0)
        l.append(mk_entries(3001, 3002, 1))
        l.close()
        l = LogStore.open(d, True)
        self.assertEqual((l.prev_index, l.last_index()), (2500, 3002))
        l.reset(5000, 9)
        l.close()
        l = LogStore.open(d, True)
        self.addCleanup(l.close)
        self.assertEqual((l.prev_index, l.prev_term, l.last_index()), (5000, 9, 5000))

    def test_term_lookups(self):
        l = LogStore.open(self.tmpdir(), True)
        self.addCleanup(l.close)
        l.append(mk_entries(1, 3, 1))
        l.append(mk_entries(4, 6, 2))
        l.append(mk_entries(7, 8, 4))
        self.assertEqual((l.last_index_of_term(2), l.first_index_of_term(2), l.last_index_of_term(3)), (6, 4, 0))


if __name__ == "__main__":
    unittest.main()
