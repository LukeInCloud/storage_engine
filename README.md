# kvstore — a persistent, replicated key/value storage engine

A network-available key/value store written in Go using **only the standard
library**. It exposes

```
Put(key, value)            Read(key)              Delete(key)
BatchPut(keys, values)     ReadKeyRange(start, end)
```

and is built as an LSM-tree storage engine (single node) plus a Raft layer
(replication to multiple nodes with automatic failover).

| Requirement | How it is met |
|---|---|
| Low latency per read/write | Bloom filters, block cache, one pinned `version` per read; writes are a WAL append + memtable insert |
| High throughput on random writes | LSM-tree: random writes become sequential I/O; group commit shares one fsync across concurrent writers; batched Raft proposals |
| Datasets much larger than RAM | Only memtable, block cache, per-file sparse indexes and bloom filters live in memory (~1.5 % of data); everything else is on disk. Tested with 2.5 GB under a 256 MiB limit |
| Crash friendliness | WAL with CRC, atomic manifest, replay on start; Raft log + hard state fsynced before replying. Tested with `kill -9` loops and chaos tests |
| Predictable behaviour | Leveled compaction with bounded merges, adaptive write-rate throttle, backpressure, no starvation in group commit |
| Replication + automatic failover | Raft: pre-vote elections, log replication, ReadIndex reads, log compaction and snapshots for lagging followers |

## Layout

```
engine/    LSM storage engine: WAL, memtable (skip list), SSTables, manifest,
           leveled compaction, block cache, group commit, throttle, stats
kvnet/     TCP protocol, server (any Store), client (single node or cluster-aware)
raft/      Raft consensus: durable log, elections, replication, snapshots, ReadIndex
replica/   Glue: Raft state machine over the engine, replicated Store, StartServer
cmd/       kvserver (standalone or cluster node), kvcli, kvbench (load generator)
```

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
go build -o bin/ ./cmd/...

# standalone
bin/kvserver -addr 127.0.0.1:7000 -dir data &
bin/kvcli -addr 127.0.0.1:7000 put city Warsaw
bin/kvcli -addr 127.0.0.1:7000 batch a 1 b 2 c 3
bin/kvcli -addr 127.0.0.1:7000 get city
bin/kvcli -addr 127.0.0.1:7000 range a b          # inclusive bounds; "" = open
bin/kvcli -addr 127.0.0.1:7000 del a

# 3-node cluster: each peer is id=raftAddr/clientAddr
P=n1=127.0.0.1:9001/127.0.0.1:7001,n2=127.0.0.1:9002/127.0.0.1:7002,n3=127.0.0.1:9003/127.0.0.1:7003
for i in 1 2 3; do bin/kvserver -id n$i -dir data$i -peers $P & done
bin/kvcli -addr 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 put k v   # follows the leader
```

Useful server flags: `-nosync` (unsafe: no fsync), `-stats 10s` (engine counters),
`-pprof 127.0.0.1:6060`.

## Semantics

* **Durability:** a write is acknowledged only after fsync (standalone) or after
  it is durable on a majority of nodes (cluster).
* **Atomicity:** `BatchPut` is one WAL record / one Raft command: all or nothing.
* **Consistency (cluster):** writes are linearizable; reads go through ReadIndex
  (leader confirms leadership with a majority, then waits for apply), so a read
  after an acknowledged write sees it, also across leader changes.
* **Failover:** followers answer `NotLeader` with the leader's client address;
  the cluster client follows it, rotates on connection failure and retries with
  backoff. Typical failover on localhost: ~0.5–1.5 s.
* **Ranges:** `ReadKeyRange(start, end)` is inclusive on both ends; `nil` bound
  = open. Keys must be non-empty; keys beginning with `\x00raft/` are reserved.
* Client retries can apply a write twice if the first attempt's outcome was
  unknown. All operations are idempotent (last write wins).

## Design notes

**Write path (engine).** Writers queue; one becomes the leader of a *group*,
appends all queued batches to the WAL with one `write` + one `fsync`, applies
them to the memtable and wakes the others. A leader runs exactly one round and
hands over (see "bugs found by benchmarking"). A full memtable is frozen and
flushed to a level-0 SSTable in the background.

**Read path.** memtable → immutable memtable → level-0 files newest first →
one binary-searched file per deeper level. Bloom filters skip files; a
byte-bounded LRU caches decoded blocks. Each read pins an immutable `version`
(one atomic increment); files replaced by compaction are deleted when the last
version referencing them is released.

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
durable by a background `Sync`; the log is compacted only up to the durable
index. A follower that falls behind the retained log is caught up by a **fuzzy
snapshot**: the leader streams its state in chunks while still applying
commands, anchored at an index; replaying the retained log after that index
repairs the fuzziness (puts and deletes are idempotent). The follower builds the
new state in a side database and swaps it in with a crash-safe commit point.

## Testing

```bash
go test -race ./...            # ~1.5 min
go test -short ./...           # skips the slow crash/chaos tests
```

* Engine: model-based randomized test against a map (with restarts), torn and
  corrupt WAL tails, orphan-file cleanup, compaction invariants under concurrent
  readers/writers, version pinning, `CompactAll`, and **`TestKill9Loop`**: a
  writer process is `SIGKILL`ed 25 times at random moments (inside WAL appends,
  flushes and compactions) and every acknowledged operation must survive.
* Raft: elections, replication, identical apply order on all nodes, failover,
  partitions (uncommitted writes of a deposed leader vanish), catch-up from the
  log and by snapshot, restarts, ReadIndex, pre-vote.
* Cluster (real TCP + real engine): failover and rejoin, whole-cluster restart,
  snapshot catch-up, linearizability under leader kill, and
  **`TestChaosKillRestartNodes`**: writers run while nodes are repeatedly
  stopped and restarted; every acknowledged write must be on every replica.

## Benchmarks

Apple Silicon laptop (10 cores, 16 GB, one SSD), macOS, everything on
localhost. `cmd/kvbench`; keys are hashed so insertion order is random.
Latencies are per request. **fsync on macOS does not flush the drive cache
(`F_FULLFSYNC` is not used), so durable-write numbers are optimistic.**

| Scenario | Throughput | p50 | p99 |
|---|---|---|---|
| standalone, fsync, 100 B random writes, 1 client | 275 /s | 4.1 ms | 5.3 ms |
| same, 64 clients (group commit) | 8.3 k/s | 8.2 ms | 12.6 ms |
| same, 256 clients | 31 k/s | 8.2 ms | 17.9 ms |
| standalone, no fsync, 128 clients | 117 k/s | 0.9 ms | 5.3 ms |
| random reads, 1.5 M keys, 32 clients | 107 k/s | 0.28 ms | 0.94 ms |
| 3-node cluster, writes, 1 client | 90 /s | 10.6 ms | 23 ms |
| 3-node cluster, writes, 256 clients | 5.7 k/s | 46 ms | 85 ms |
| 3-node cluster, writes, 64 clients × batch of 50 | 72 k items/s | 42 ms | 101 ms |
| 3-node cluster, linearizable reads, 32 clients | 59 k/s | 0.47 ms | 1.9 ms |
| 3-node cluster, **no fsync**, writes, 64 clients | 48 k/s | 0.8 ms | 12.6 ms |

The cluster's fsync-bound write numbers are limited by three nodes sharing one
disk (each commit needs an fsync on a majority); with fsync off the same code
does 48–58 k writes/s at 0.14 ms for a single client.

**Larger than memory.** 2.5 M keys × 1000 B ≈ 2.4 GB written in random order
(batches of 50, 16 clients) to a server run with `GOMEMLIMIT=256MiB`; the 32 MiB
block cache is the only large buffer (peak RSS ≈ 180 MB):

| Phase | Throughput | p50 | p99 |
|---|---|---|---|
| fill (sustained, ~82 s, disk-bound) | 30 k items/s | 25 ms | 60 ms (p99.9 78 ms) |
| random reads, 32 clients | 48 k/s | 0.51 ms | 3.4 ms |
| random reads, 1 client | 8.7 k/s | 0.13 ms | 0.22 ms |
| range scans of ~100 keys, 8 clients | 591 k items/s | 1.2 ms | 4.5 ms |
| mixed 50/50, 32 clients | 7.9 k/s | 4.9 ms | 15 ms |

The data set is cached by the OS (16 GB RAM), so reads here are not cold-disk
reads. During the fill, no second ran below 17 k items/s and there were no hard
stalls; compaction keeps the (disk-bound, ~280 MB/s) merge thread busy.

### Bugs and design problems the benchmarks found

1. **Group-commit leader starvation.** A leader kept serving the queue until it
   was empty, so under continuous load its own caller never got a response (a
   30 s client timeout). Now a leader runs one round and hands off. Regression
   test included.
2. **Size-tiered compaction froze writers** for ~0.5 s per giant merge (p99
   480 ms, 26 of 75 seconds below 20 k items/s). Replaced by leveled
   compaction plus the adaptive throttle → p99 55 ms, p99.9 110 ms.
3. **Cluster writes were 19 ms** because the leader fsynced the engine on the
   critical path although the Raft log already guarantees durability. Now
   acknowledged at commit, with background `Sync` and log compaction bounded by
   the durable index → 10.6 ms.
4. The chaos test found Raft RPC handlers running against a closed log after
   `Stop`.

## Limitations (what a production system would still need)

* **Static membership**: the node set is fixed at start; no joint consensus.
* One Raft group holds the whole key space (no sharding), so write throughput is
  that of one leader.
* No TLS or authentication; no CheckQuorum, lease reads or follower reads.
* No compression; the in-memory block index costs ~1.5 % of the data size.
* One compaction at a time (merges are bounded, but a larger machine could run
  several in parallel); the write rate is bounded by compaction throughput.
* `ReadKeyRange` returns the whole result in one response (capped at 64 MiB).
* The Raft log keeps up to `LogRetain` (8192) entries in memory.
* fsync semantics are those of the OS (see the macOS note above).
