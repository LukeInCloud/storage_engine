"""kvstore: a persistent, replicated key/value storage engine (standard library only).

Sub-packages:
    engine   LSM storage engine (WAL, memtable, SSTables, manifest, compaction)
    kvnet    TCP protocol, server and client
    raft     Raft consensus (durable log, elections, replication, snapshots)
    replica  glue: Raft state machine over the engine, replicated store
    cmd      kvserver, kvcli, kvbench entry points (python3 -m kvstore.cmd.kvserver)
"""
