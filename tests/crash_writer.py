"""Writer process for test_crash: python3 -m tests.crash_writer DIR START.

Prints the index of every acknowledged operation on stdout; it is killed with
SIGKILL at random moments by the test."""

import os
import sys

from kvstore.engine import DB, Options


def crash_key(i: int) -> bytes:
    return b"key-%07d" % i


def crash_op(i: int):
    """op(i) is deterministic: replaying a prefix always yields the same state,
    so the expected state is a pure function of how many operations are acked."""
    if i % 10 == 9:
        return crash_key(i - 5), b"", True
    return crash_key(i), b"v%d-" % i + b"x" * (100 + i % 97), False


def expected_state(n: int) -> dict:
    m = {}
    for i in range(n):
        k, v, delete = crash_op(i)
        if delete:
            m.pop(k, None)
        else:
            m[k] = v
    return m


def main() -> None:
    directory, start = sys.argv[1], int(sys.argv[2])
    # Tiny sizes so kills also land inside multi-level compactions. fsync is on.
    db = DB.open(directory, Options(memtable_size=16 << 10, compaction_trigger=3,
                                    target_file_size=24 << 10, level_base_bytes=64 << 10))
    out = sys.stdout.buffer
    i = start
    while True:
        k, v, delete = crash_op(i)
        if delete:
            db.delete(k)
        else:
            db.put(k, v)
        out.write(b"%d\n" % i)  # acknowledged
        out.flush()
        i += 1


if __name__ == "__main__":
    main()
