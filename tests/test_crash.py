import os
import random
import subprocess
import sys
import time
import unittest

from kvstore.engine import DB, Options
from tests.crash_writer import expected_state
from tests.helpers import TempDirTestCase, short

ROUNDS = 12


def same_state(db, want: dict) -> bool:
    got = db.read_key_range(None, None)
    return len(got) == len(want) and all(want.get(kv.key) == kv.value for kv in got)


@unittest.skipIf(short(), "slow")
class TestKill9Loop(TempDirTestCase):
    def test_kill9_loop(self):
        """A writer process is SIGKILLed at random moments (inside WAL appends,
        flushes and compactions); after every kill the database is reopened and
        every acknowledged operation must have survived."""
        d = self.tmpdir()
        rnd = random.Random(time.time_ns())
        acked = 0  # operations [0, acked) are acknowledged
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for rnd_i in range(ROUNDS):
            p = subprocess.Popen([sys.executable, "-m", "tests.crash_writer", d, str(acked)],
                                 stdout=subprocess.PIPE, cwd=root)
            time.sleep(0.4 + rnd.random() * 0.8)
            p.kill()  # SIGKILL: no chance to flush or clean up
            out = p.stdout.read()
            p.wait()
            p.stdout.close()
            for line in out.split(b"\n"):
                if line.strip().isdigit():
                    acked = max(acked, int(line) + 1)

            db = DB.open(d, Options(no_sync=True, compaction_trigger=-1))
            try:
                # The op that was in flight at kill time may or may not have been applied.
                ok = same_state(db, expected_state(acked)) or same_state(db, expected_state(acked + 1))
            finally:
                db.close()
            self.assertTrue(ok, "round %d: state after crash matches neither %d nor %d acked ops"
                            % (rnd_i, acked, acked + 1))
        self.assertGreaterEqual(acked, 100, "writer made too little progress to be meaningful")


if __name__ == "__main__":
    unittest.main()
