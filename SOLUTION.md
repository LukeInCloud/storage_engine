
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
* fsync semantics are those of the OS (tests on multiple Operating Systems)
* Tests on real network cluster (so far tests were only on `localhost`)
* Backport Python changes to Go implementation and compare performance on cluster.
* Testing and handling edge cases.

## AI Usage (a note from human)

Essentially, this code was created in three iterations using Claude Code (model: Sonnet 5.5):

> AI Prompts

### 1st session - Planning

1. Add context to the AI:

> Analyze the articles below and describe to me in Polish what they are about, as well as the similarities and differences between the solutions described.

2. Plan the solution:

> Based on them, propose "the storage engine which is responsible for the maintenance of persistent data within the database management system."
"The objective of this task is to implement a network-available persistent Key/Value system
that exposes the interfaces listed below in any programming language of choice. You should rely only on the standard libraries of the language."
#Interface
1 Put(Key, Value)
2 Read(Key)
3 ReadKeyRange(StartKey, EndKey)
4 BatchPut(..keys, ..values)
5 Delete(key)
The implementation should strive to achieve the following requirements:
● Low latency per item read or written
● High throughput, especially when writing an incoming stream of random items
● Ability to handle datasets much larger than RAM without degradation
● Crash friendliness, both in terms of fast recovery and not losing data
● Predictable behavior under heavy access load or large volume
● Replicate data to multiple nodes
● Handle automatic failover to the other nodes
Suggest the language in which the implementation should be carried out

3. Decisions and implementation

The architecture proposed by AI looked good for me.
The AI proposed implementation in Go or Rust.
I took Go because had sufficient **standard library**.
Then Claude implemented the code on the branch `go` (with commits after each step).

Cost: **~353k tokens**

### 2nd session - Implementation

As `Go` is not my primary language and I wanted more control over the code,
I requested AI to rewrite the code to the `python` branch and implement exact same solution in Python 3.x.
As the performance results are significantly worse than in Go, the production solution
probably would stay in Go (or Rust if we could use external libraries).
I'd consider also modern Python versions with modified `Garbage collector`.

Cost: **~313k tokens**

### 3rd session - Testing & improvements

I tested manually only Python version.
Id didn't like that we have to know all nodes when we're starting the cluster,
so I requested to implement "gossip protocol" - to notify cluster with newly created nodes
and remove dead nodes from the poll.

Cost: **~205k tokens**
