import os
from dataclasses import dataclass, field

from ..util import write_file_atomic

NUM_LEVELS = 7
MANIFEST_NAME = "MANIFEST"


@dataclass
class Manifest:
    """The single source of truth about which files make up the DB.

        log N          WAL files numbered < N are fully flushed to SSTables
        table LEVEL ID SSTables per level; within level 0 oldest first (newer
                       files shadow older ones), in other levels any order

    It is replaced atomically (write temp, fsync, rename, fsync dir).
    """

    log_num: int = 0
    levels: list = field(default_factory=lambda: [[] for _ in range(NUM_LEVELS)])


def write_manifest(directory: str, m: Manifest) -> None:
    lines = ["log %d\n" % m.log_num]
    for level, ids in enumerate(m.levels):
        for fid in ids:
            lines.append("table %d %d\n" % (level, fid))
    write_file_atomic(os.path.join(directory, MANIFEST_NAME), "".join(lines).encode(), directory)


def read_manifest(directory: str) -> Manifest:
    m = Manifest()
    try:
        with open(os.path.join(directory, MANIFEST_NAME), "rb") as f:
            data = f.read().decode()
    except FileNotFoundError:
        return m
    for line in data.strip().split("\n"):
        if line == "":
            continue
        f = line.split()
        try:
            if len(f) == 2 and f[0] == "log":
                m.log_num = int(f[1])
            elif len(f) == 3 and f[0] == "table":
                level, fid = int(f[1]), int(f[2])
                if level < 0 or level >= NUM_LEVELS or fid < 0:
                    raise ValueError
                m.levels[level].append(fid)
            else:
                raise ValueError
        except ValueError:
            raise ValueError("manifest: bad line %r" % line) from None
    return m
