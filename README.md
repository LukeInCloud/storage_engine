# kvstore — a persistent, replicated key/value storage engine

A network-available key/value store written in Python 3 using **only the
standard library**. It exposes

```
put(key, value)            read(key)              delete(key)
batch_put(keys, values)    read_key_range(start, end)
```

and is built as an LSM-tree storage engine (single node) plus a Raft layer
(replication to multiple nodes with automatic failover).

This is a port of the Go implementation on the `go` branch: same architecture,
same on-disk and wire concepts, same test strategy.

| Requirement | How it is met |
|---|---|
| Low latency per read/write | Bloom filters, block cache, one pinned `Version` per read; writes are a WAL append + memtable insert |
| High throughput on random writes | LSM-tree: random writes become sequential I/O; group commit shares one fsync across concurrent writers; batched Raft proposals |
| Datasets much larger than RAM | Only memtable, block cache, per-file sparse indexes and bloom filters live in memory; everything else is on disk; scans stream in pages |
| Crash friendliness | WAL with CRC, atomic manifest, replay on start; Raft log + hard state fsynced before replying. Tested with `kill -9` loops and chaos tests |
| Predictable behaviour | Leveled compaction with bounded merges, adaptive write-rate throttle, backpressure, no starvation in group commit |
| Replication + automatic failover | Raft: pre-vote elections, log replication, ReadIndex reads, log compaction and snapshots for lagging followers |

## Layout

```
kvstore/engine/    LSM storage engine: WAL, memtable, SSTables, manifest,
                   leveled compaction, block cache, group commit, throttle, stats
kvstore/kvnet/     TCP protocol, server (any store), client (single node or cluster-aware)
kvstore/raft/      Raft consensus: durable log, elections, replication, snapshots, ReadIndex,
                   membership changes (learners, single-server additions)
kvstore/gossip/    Gossip membership/liveness protocol used to discover and introduce nodes
kvstore/replica/   Glue: Raft state machine over the engine, replicated Store, start_server
kvstore/cmd/       kvserver, kvcli, kvbench (load generator)
tests/             unittest suites for every layer
```

Requires Python 3.10+ (tested on 3.14). No third-party packages.

How the source papers map onto the code:

* **LSM-tree, Bigtable** — memtable → immutable sorted SSTables (blocks, sparse
  index, bloom filter), tombstones, minor compaction (flush) and merging
  compactions; leveled compaction is the paper's "rolling merge" across
  components that grow by a constant factor.
* **Bitcask** — append-only files, CRC-protected records, recovery by replaying
  a log, files never modified in place. (Its all-keys-in-RAM index was *not*
  adopted: it breaks "larger than RAM" and range scans.)
* **Raft (and Paxos)** — `raft/` implements Raft; Paxos provides the safety
  argument (majority quorums), Raft the understandable protocol.

## Quick start

```bash
# standalone
python3 -m kvstore.cmd.kvserver -addr 127.0.0.1:7000 -dir data &
python3 -m kvstore.cmd.kvcli -addr 127.0.0.1:7000 put city Warsaw
python3 -m kvstore.cmd.kvcli -addr 127.0.0.1:7000 batch a 1 b 2 c 3
python3 -m kvstore.cmd.kvcli -addr 127.0.0.1:7000 get city
python3 -m kvstore.cmd.kvcli -addr 127.0.0.1:7000 range a b          # inclusive bounds; "" = open
python3 -m kvstore.cmd.kvcli -addr 127.0.0.1:7000 del a

# 3-node cluster: each peer is id=raftAddr/clientAddr
P=n1=127.0.0.1:9001/127.0.0.1:7001,n2=127.0.0.1:9002/127.0.0.1:7002,n3=127.0.0.1:9003/127.0.0.1:7003
for i in 1 2 3; do python3 -m kvstore.cmd.kvserver -id n$i -dir data$i -peers $P & done
python3 -m kvstore.cmd.kvcli -addr 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 put k v   # follows the leader

# add a 4th node: it is told about ONE existing member (any of them) and nothing else
python3 -m kvstore.cmd.kvserver -id n4 -dir data4 -addr 127.0.0.1:7004 -raft 127.0.0.1:9004 \
        -join 127.0.0.1:9002 &
# -id/-addr/-raft without -peers and -join starts a one-node cluster; restarts need none of
# these flags beyond -id/-addr/-raft (the membership is persisted in the node's directory)
```

Useful server flags: `-nosync` (unsafe: no fsync), `-stats 10s` (engine counters).
Both `-flag` and `--flag` spellings are accepted.

As a library:

```python
from kvstore.engine import DB, Options, NotFoundError

with DB.open("data", Options(no_sync=False)) as db:
    db.put(b"k", b"v")
    db.batch_put([b"a", b"b"], [b"1", b"2"])
    print(db.read(b"k"))
    print(db.read_key_range(b"a", b"b"))      # [KV(key=b'a', ...), KV(key=b'b', ...)]
    db.delete(b"k")                           # db.read(b"k") now raises NotFoundError
```

## Semantics

* **Durability:** a write is acknowledged only after fsync (standalone) or after
  it is durable on a majority of nodes (cluster).
* **Atomicity:** `batch_put` is one WAL record / one Raft command: all or nothing.
* **Consistency (cluster):** writes are linearizable; reads go through ReadIndex
  (leader confirms leadership with a majority, then waits for apply), so a read
  after an acknowledged write sees it, also across leader changes.
* **Failover:** followers answer `NotLeader` with the leader's client address;
  the cluster client follows it, rotates on connection failure and retries with
  backoff. Measured failover after `kill -9` of the leader on localhost: ~1.8 s
  for the first write (election timeout 400–800 ms by default plus client
  backoff).
* **Ranges:** `read_key_range(start, end)` is inclusive on both ends; `None`
  bound = open. Keys must be non-empty; keys beginning with `\x00raft/` are reserved.
* Client retries can apply a write twice if the first attempt's outcome was
  unknown. All operations are idempotent (last write wins).

## Design notes

**Write path (engine).** Writers queue; one becomes the leader of a *group*,
appends all queued batches to the WAL with one `write` + one `fsync`, applies
them to the memtable and wakes the others. A leader runs exactly one round and
hands over (see "bugs found by benchmarking"). A full memtable is frozen and
flushed to a level-0 SSTable by a background thread.

**Read path.** memtable → immutable memtable → level-0 files newest first →
one binary-searched file per deeper level. Bloom filters skip files; a
byte-bounded LRU caches decoded blocks. Each read pins an immutable `Version`
(one reference-count increment); files replaced by compaction are deleted when
the last version referencing them is released. Range scans are a lazy k-way
merge (`heapq.merge`) over generators, one per memtable/L0 file/deeper level.

**Compaction.** Level 0 holds overlapping flushed files; levels 1–6 hold sorted,
non-overlapping files, each level 10× larger. A compaction merges one file (or
all of L0) with the overlapping files one level down, so its cost is bounded by
a few tens of MiB regardless of database size. Single-file moves without overlap
are done without rewriting. Tombstones are dropped only when no deeper level can
contain the key. Backpressure: an adaptive AIMD throttle lowers the accepted
write rate while level 0 keeps growing; a hard stall (3× trigger) is only a last
resort.

**Raft.** Log and hard state on disk (CRC'd records, torn-tail safe). Pre-vote
keeps a partitioned node from deposing a healthy leader. Proposals are batched
and fsynced once; they are acknowledged at *commit*. The state machine
(`replica.KVMachine`) writes the applied index inside the same atomic engine
batch as the command's effects, runs without per-apply fsync and is made
durable by a background `sync`; the log is compacted only up to the durable
index. A follower that falls behind the retained log is caught up by a **fuzzy
snapshot**: the leader streams its state in chunks while still applying
commands, anchored at an index; replaying the retained log after that index
repairs the fuzziness (puts and deletes are idempotent). The follower builds the
new state in a side database and swaps it in with a crash-safe commit point.

**Adding nodes (gossip + Raft).** A new node is configured with the Raft
address of *one* member (`-join`). What happens:

1. *Introduction (gossip).* The node exchanges member tables with that peer
   (push-pull: it sends its own entry, it receives the peer's whole table).
   Now it knows every member's id and addresses, and the peer's table contains
   the newcomer. Every `gossip_interval` each node bumps a heartbeat and
   exchanges tables with `fanout` random peers; entries only move to higher
   versions `(generation, heartbeat)`, so tables converge, and a node that
   learns something new gossips immediately instead of waiting for its tick,
   so the news reaches N nodes in O(log N) rounds. Entries not advanced for
   `fail_after` are shown as not alive.
2. *Initial data.* Before it takes part in anything, the node pulls a copy of
   the state from the seed (`PullSnapshot`, chunked). The copy is a fuzzy
   snapshot anchored at an index of the seed's log, with the configuration in
   effect at that index; the rest is repaired by normal replication, exactly
   like a snapshot sent by a leader. If the seed cannot serve it, the leader
   sends a snapshot instead.
3. *Raft membership.* Each leader periodically compares the gossip table with
   the Raft configuration and adds unknown, live nodes with a configuration
   log entry (**single-server change**: one change at a time, only after an
   entry of the leader's own term has committed). The node is first a
   *learner*: it is replicated to but is not counted for quorums and never
   campaigns, so a node that is still copying data cannot stall commits or
   disturb elections. When it is within one batch of the leader's log and
   answering, the leader promotes it to voter with a second entry.
4. A configuration takes effect when it is *appended* (not committed); a
   follower that has a conflicting suffix truncated reverts to the previous
   one. Entries carry the members' addresses, so every node learns them from
   the log even if gossip is late. The configuration at the start of the
   retained log is persisted in `raft/members` before the log is compacted
   past its entry, and travels in snapshots.

**Removing dead nodes.** Gossip alone never removes anybody; the cluster
agrees first. A member that has been silent (its gossip heartbeat has not
advanced) for `dead_after` (`-dead-after`, default 60 s, 0 disables) is a
candidate. The leader asks the other voters whether *they* also consider it
silent (`DeadQuery` RPC) and proceeds only if a majority of the current
voters, itself included, confirm. A leader cut off from the majority therefore
can never shrink the cluster to a quorum of its own. It then appends a
configuration entry without the node (one change at a time, like additions).
Every node applies the entry: the node leaves its peer lists, replicator
threads, address tables and gossip table (a tombstone makes gossip ignore it
until it returns with a newer generation, i.e. after a restart, when it is
treated as a new joiner). Removed learners and voters are handled alike.

### Differences from the Go version

The Python port keeps the algorithms and file layouts, and changes what the
language and standard library make sensible:

| Area | Go | Python |
|---|---|---|
| Concurrency | goroutines, channels, `sync.RWMutex` | threads, `Condition`/`Event`, a small `RWLock`; blocking sockets with one thread per connection |
| Checksums | CRC-32C (Castagnoli) | `zlib.crc32` (CRC-32/IEEE, computed in C — the standard library has no CRC-32C) |
| Bloom hash | FNV-1a loop | BLAKE2b-64 via `hashlib` (a per-byte Python loop would dominate read cost) |
| Memtable | skip list | hash map + sorted key array (`bisect`): the searching and shifting run in C, which beats a pointer-chasing skip list in CPython; scans iterate a snapshot |
| Iterators | `Valid/Next/Entry` structs, hand-written merge heap | generators + `heapq.merge`, newest source wins ties |
| Peer RPC | `net/rpc` + gob | length-prefixed frames with a compact hand-written binary encoding, a small connection pool per peer |
| Errors | sentinel errors | exception classes (`NotFoundError`, `NotLeaderError`, …) |
| Profiling | `-pprof` | not provided |

Everything else — WAL record format, manifest, SSTable layout, level sizing,
throttle constants, Raft message semantics, the `\x00raft/applied` marker,
snapshot sessions — is the same.

## Testing

```bash
python3 -m unittest discover -s tests -t .              # ~1 min
KV_SHORT=1 python3 -m unittest discover -s tests -t .   # skips the slow crash/chaos tests
python3 -m unittest tests.test_raft -v                  # one suite
```

* Engine: model-based randomized test against a dict (with restarts), torn and
  corrupt WAL tails, orphan-file cleanup, compaction invariants under concurrent
  readers/writers, version pinning, `compact_all`, group-commit durability and
  starvation, and **`TestKill9Loop`**: a writer process is `SIGKILL`ed 12 times
  at random moments (inside WAL appends, flushes and compactions) and every
  acknowledged operation must survive.
* Membership: gossip convergence from a single seed, a late joiner known to
  everybody, liveness and restarts (generation), address changes; Raft learner
  promotion, a new voter completing quorums, configuration carried by snapshots
  and persisted across restarts, one change at a time, only the leader changes;
  real TCP nodes joining through one peer (also chained: each new node knows
  only the previous one), copying data, becoming voters, surviving a leader kill and a
  restart without any join flag, and a seed that is down when the node starts;
  removal of a dead node (only with majority confirmation, not after a short
  outage or while the quorum is lost), persisted across restarts and compaction.
* Raft: elections, replication, identical apply order on all nodes, failover,
  partitions (uncommitted writes of a deposed leader vanish), catch-up from the
  log and by snapshot, restarts, ReadIndex, pre-vote, log never compacted past
  the durable index; storage tests for hard state and the log file.
* Cluster (real TCP + real engine): failover and rejoin, whole-cluster restart,
  snapshot catch-up, linearizability under leader kill, and
  **`TestChaos.test_chaos_kill_restart_nodes`**: writers run while nodes are
  repeatedly stopped and restarted; every acknowledged write must be on every
  replica.

## Benchmarks

`kvstore/cmd/kvbench.py`; keys are hashed so insertion order is random. Latencies
are per request. Measured on an Apple Silicon laptop, Python 3.14, everything on
localhost, short runs (1–5 s each), one `kvbench` process per scenario — so the
load generator itself is bound by CPython's global interpreter lock and these
numbers are indicative, not a ceiling.

**Caveat:** on this machine `fsync` returns in ~30 µs (measured separately), and
macOS `fsync` does not flush the drive cache (`F_FULLFSYNC` is not used), so
"durable" numbers here are far more optimistic than on a disk with real flush
latency. Group commit's value shows when fsync is slow; here it mostly amortises
syscalls.

| Scenario | Throughput | p50 | p99 |
|---|---|---|---|
| standalone, fsync, 100 B random writes, 1 client | 16.7 k/s | 49 µs | 108 µs |
| same, 64 clients (group commit) | 25.9 k/s | 2.2 ms | 5.8 ms |
| same, 256 clients | 25.1 k/s | 8.2 ms | 30 ms |
| standalone, no fsync, 128 clients | 26.4 k/s | 4.5 ms | 13.8 ms |
| standalone, no fsync, 16 clients × batch of 50 | 123 k items/s | 4.1 ms | 35.7 ms |
| random reads, 60 k keys, 32 clients | 31.8 k/s | 0.94 ms | 3.2 ms |
| range scans of ~100 keys, 8 clients | 1.4 M items/s | 2.7 ms | 7.5 ms |
| 3-node cluster, writes, 1 client | 4.7 k/s | 0.18 ms | 0.28 ms |
| 3-node cluster, writes, 64 clients | 20.4 k/s | 2.9 ms | 5.8 ms |
| 3-node cluster, writes, 64 clients × batch of 50 | 115 k items/s | 21 ms | 120 ms |
| 3-node cluster, linearizable reads, 32 clients | 25.1 k/s | 1.2 ms | 2.4 ms |

The server side is a single CPython process per node: the GIL serialises
Python bytecode, so throughput is bounded by one core per process, and the
Go version's multi-core numbers are not reachable. Batching (`batch_put`) is the
way to push high item rates, since per-request overhead dominates.

### Bugs and design problems the Go version's benchmarks found (kept fixed here)

1. **Group-commit leader starvation.** A leader kept serving the queue until it
   was empty, so under continuous load its own caller never got a response. Now
   a leader runs one round and hands off. Regression test included.
2. **Size-tiered compaction froze writers** for giant merges. Replaced by
   leveled compaction plus the adaptive throttle.
3. **Cluster writes paid an engine fsync on the critical path** although the
   Raft log already guarantees durability. Now acknowledged at commit, with
   background `sync` and log compaction bounded by the durable index.
4. Raft RPC handlers must not run against a closed log after `stop`.

## Limitations (what a production system would still need)

* **Removal is automatic only for dead nodes**: there is no operator command
  to remove a healthy node, and a node's addresses cannot change. A partitioned
  node that was removed and then reconnects with the same process is ignored
  until it restarts. Removing a node when the cluster is already without
  quorum is impossible (it needs a commit). No joint consensus (not needed for
  one-at-a-time changes).
* **Joining is unauthenticated**: any process that can reach a member's Raft
  port and gossips a table entry is added to the cluster, and can pull a copy
  of the data.
* Gossip exchanges the whole table (fine for tens of nodes; use digests beyond).
* One Raft group holds the whole key space (no sharding), so write throughput is
  that of one leader.
* No TLS or authentication; no CheckQuorum, lease reads or follower reads.
* No compression; the in-memory block index costs ~1.5 % of the data size.
* One compaction at a time; the write rate is bounded by compaction throughput.
* `read_key_range` returns the whole result in one response (capped at 64 MiB).
* The Raft log keeps up to `log_retain` (8192) entries in memory.
* CPython: one core per process (GIL); decoded blocks and entries are Python
  objects, so memory per cached byte is higher than in the Go version — size
  `block_cache_size` accordingly.
* fsync semantics are those of the OS (see the macOS note above).
