import os
import threading
import time

from ..util import sync_dir
from .errors import AbortedError, ClosedError
from .codec import KIND_DELETE
from .manifest import NUM_LEVELS, write_manifest
from .merge import merge_entries
from .sstable import SSTWriter, finish_table, sst_path
from .version import Version, level_iter, overlapping

DEFAULT_TARGET_FILE_SIZE = 8 << 20
DEFAULT_LEVEL_BASE = 64 << 20  # maximum size of level 1
LEVEL_MULTIPLIER = 10


class Compaction:
    """One unit of work: merge the files `from_` of `level` with the
    overlapping files `into` of level+1."""

    def __init__(self, level, from_, into, trivial):
        self.level = level
        self.from_ = from_  # newest first for level 0
        self.into = into
        self.trivial = trivial  # a single file with no overlap just moves down, no rewrite

    def key_range(self):
        files = self.from_ + self.into
        return min(f.smallest for f in files), max(f.largest for f in files)


class CompactionMixin:
    def max_bytes_for_level(self, level: int) -> int:
        n = self.opts.level_base_bytes
        for _ in range(1, level):
            n *= LEVEL_MULTIPLIER
        return n

    def _compaction_enabled(self) -> bool:
        return self.opts.compaction_trigger >= 2

    def _pick_compaction(self, v: Version):
        """Choose the most urgent job, or None. Level 0 is scored by file count
        (reads must probe every L0 file), deeper levels by size over capacity."""
        best, best_score = -1, 0.0
        s = len(v.levels[0]) / self.opts.compaction_trigger
        if s >= 1:
            best, best_score = 0, s
        for l in range(1, NUM_LEVELS - 1):
            s = v.level_bytes(l) / self.max_bytes_for_level(l)
            if s >= 1 and s > best_score:
                best, best_score = l, s
        if best < 0:
            return None
        return self._compaction_for(v, best)

    def _compaction_for(self, v: Version, l: int):
        """The job that pushes data out of level l."""
        files = v.levels[l]
        if not files:
            return None
        if l == 0:
            from_ = list(reversed(files))  # newest first
        else:
            # Round-robin through the key space so every file gets its turn.
            pick = files[0]
            ptr = self.compact_ptr[l] or b""
            for f in files:
                if f.smallest > ptr:
                    pick = f
                    break
            from_ = [pick]
        lo = min(f.smallest for f in from_)
        hi = max(f.largest for f in from_)
        into = overlapping(v.levels[l + 1], lo, hi)
        return Compaction(l, from_, into, len(into) == 0 and len(from_) == 1)

    # ---- background loop ----

    def _compact_loop(self) -> None:
        while self.compact_sig.wait():
            while True:
                try:
                    did = self._compact_once()
                except AbortedError:
                    break
                except Exception as e:
                    self.fail(e)
                    break
                if not did:
                    break

    def _compact_once(self) -> bool:
        if not self._compaction_enabled():
            return False
        with self.compact_mu:
            v = self._pin_version()
            if v is None:
                return False
            try:
                c = self._pick_compaction(v)
                if c is None:
                    return False
                self._run_compaction(v, c)
                return True
            finally:
                v.unref()

    def _pin_version(self):
        """The current version with a reference held, or None when closed/failed."""
        with self.mu:
            if self.closed or self.failed is not None:
                return None
            self.cur.ref()
            return self.cur

    def _alloc_num(self) -> int:
        with self.mu:
            n = self.next_num
            self.next_num += 1
            return n

    def _compact_iter(self, merged, drop_tombs: bool):
        """Filter a merged stream: optionally drop tombstones and check for
        shutdown every few entries so Close is never held up by a long merge."""
        n = 0
        for e in merged:
            n += 1
            if n % 1024 == 0 and self._is_closed():
                raise AbortedError()
            if drop_tombs and e.kind == KIND_DELETE:
                continue
            yield e

    def _run_compaction(self, v: Version, c: Compaction) -> None:
        t0 = time.monotonic()
        read = wrote = 0
        try:
            remove = set(c.from_) | set(c.into)
            add = [[] for _ in range(NUM_LEVELS)]
            outputs = []
            if c.trivial:
                add[c.level + 1] = list(c.from_)  # same files, one level down
                remove = {c.from_[0]}
            else:
                lo, hi = c.key_range()
                bottom = True  # no deeper level has data for this key range
                for l in range(c.level + 2, NUM_LEVELS):
                    if overlapping(v.levels[l], lo, hi):
                        bottom = False
                        break
                srcs = [f.iter(None, False) for f in c.from_]  # newest first
                if c.into:
                    srcs.append(level_iter(c.into, None, False))
                it = self._compact_iter(merge_entries(srcs), bottom)
                outputs = self._write_outputs(it)
                add[c.level + 1] = outputs
                read = sum(t.size for t in remove)
                wrote = sum(t.size for t in outputs)

            with self.mu:
                if self.closed or self.failed is not None:
                    self._discard(outputs)
                    raise AbortedError()
                # Flushes only add level-0 files while we worked; our inputs must still be there.
                present = {t for files in self.cur.levels for t in files}
                for t in remove:
                    if t not in present:
                        self._discard(outputs)
                        raise RuntimeError("engine: compaction input %06d vanished" % t.id)
                levels = self.cur.with_changes(remove, add)
                try:
                    self._install_version(levels, self.man.log_num, outputs, remove, c.trivial)
                except BaseException:
                    self._discard(outputs)
                    raise
                if c.level > 0:
                    self.compact_ptr[c.level] = c.from_[-1].largest
            if c.trivial:
                self.ctr.add("trivial_moves")
            else:
                self.ctr.add("compactions")
                self.ctr.add("compact_bytes_read", read)
                self.ctr.add("compact_bytes_write", wrote)
        finally:
            self.ctr.add("compact_nanos", int((time.monotonic() - t0) * 1e9))

    def _install_version(self, levels, log_num, fresh, removed, moved) -> None:
        """Make levels current. Caller holds self.mu. The manifest is written
        first, so a crash leaves either the old or the new layout. `fresh` are
        newly created tables whose creator reference is handed to the version;
        removed tables are deleted once their last reader is gone (unless moved)."""
        nv = Version(levels)
        man = nv.manifest(log_num)
        try:
            write_manifest(self.dir, man)
        except BaseException:
            nv.unref()
            raise
        self.man = man
        if not moved and removed:
            for t in removed:
                t.obsolete = True
        old = self.cur
        self.cur = nv
        old.unref()
        for t in fresh:
            t.unref()  # creator's reference; the version holds its own
        self.cond.notify_all()

    @staticmethod
    def _discard(outs) -> None:
        """Delete output files that never became part of a version."""
        for t in outs:
            t.obsolete = True
            t.unref()

    def _write_outputs(self, it):
        """Write the merged stream as files of about target_file_size each."""
        outs = []
        w = None
        path = None
        fid = 0
        try:
            for e in it:
                if w is None:
                    fid = self._alloc_num()
                    path = sst_path(self.dir, fid)
                    w = SSTWriter(path)
                w.add(e)
                if w.size() >= self.opts.target_file_size:
                    tw, w = w, None
                    outs.append(finish_table(tw, path, fid, self.cache))
            if w is not None:
                tw, w = w, None
                outs.append(finish_table(tw, path, fid, self.cache))
            if outs:
                sync_dir(self.dir)
        except BaseException:
            if w is not None:
                w.abort()
                try:
                    os.remove(path)
                except OSError:
                    pass
            self._discard(outs)
            raise
        return outs

    def _is_closed(self) -> bool:
        with self.mu:
            return self.closed

    # ---- maintenance ----

    def compact_all(self) -> None:
        """Push all data down to the deepest level, dropping overwritten
        versions and tombstones. It is a maintenance operation (reclaims
        space); normal operation never needs it."""
        if not self._compaction_enabled():
            raise RuntimeError("engine: compaction disabled")
        self.flush()
        with self.compact_mu:
            for l in range(NUM_LEVELS - 1):
                while True:
                    v = self._pin_version()
                    if v is None:
                        raise ClosedError()
                    try:
                        if not v.levels[l]:
                            break
                        c = self._compaction_for(v, l)
                        c.trivial = False  # always rewrite so tombstones and old versions are dropped
                        self._run_compaction(v, c)
                    finally:
                        v.unref()

    def level_files(self):
        """Number of tables per level (diagnostics and tests)."""
        with self.mu:
            return [len(f) for f in self.cur.levels]

    def check_levels(self) -> None:
        """Verify the structural invariants of the layout."""
        with self.mu:
            for l in range(1, NUM_LEVELS):
                files = self.cur.levels[l]
                for i, f in enumerate(files):
                    if f.smallest > f.largest:
                        raise AssertionError("level %d file %06d: smallest > largest" % (l, f.id))
                    if i > 0 and files[i - 1].largest >= f.smallest:
                        raise AssertionError("level %d: files %06d and %06d overlap" % (l, files[i - 1].id, f.id))
