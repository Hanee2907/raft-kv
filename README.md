# raft-kv

A distributed, fault-tolerant key-value store built on the **Raft consensus algorithm**, implemented from scratch in Java 21 with no dependencies, together with a **Jepsen-style test harness** that injects network partitions and node crashes while checking that the system stays **linearizable**.

```
$ ./run-tests.sh
PASS  checker-rejects-invalid-histories
PASS  isolated-leader-writes-are-discarded
PASS  snapshot-compaction-and-install
PASS  jepsen-partitions-and-crashes-seed-1
      ops: 1446 ok, 6 failed, 31 indeterminate | terms with a leader: 7 | snapshots taken/installed: 93/10 | checker: LINEARIZABLE
PASS  jepsen-negative-control-stale-reads-are-caught
PASS  tcp-cluster-with-file-storage
...
16 passed, 0 failed
```

## What's implemented

| Area | Details |
|---|---|
| **Leader election** | Randomised election timeouts, terms, one vote per term, the "candidate's log must be at least as up to date" rule, no-op entry on election so earlier-term entries commit safely (§5.4.2). |
| **Log replication** | `AppendEntries` with consistency check, fast conflict backtracking (term + index hints instead of one step per round trip), commit by majority, in-order apply. |
| **Snapshots** | Log compaction once N entries are applied; `InstallSnapshot` for followers that have fallen behind the compacted prefix; recovery from snapshot + log suffix after restart. |
| **Durability** | State is persisted *before* any message that depends on it is sent. Crash-safe file storage: temp file, fsync, atomic rename, directory fsync, CRC checksum. |
| **Linearizable reads and writes** | GET, PUT and CAS all go through the log. Retried requests are de-duplicated per `(clientId, seq)` so a retry applies at most once. |
| **Transport** | `Transport` is a three-line interface. Shipped: TCP with a hand-written binary codec (no Java serialization), per-peer send queues and reconnect. Also an in-process simulated network for tests. |
| **Server and client** | `KvServer` (Raft node + TCP + disk + a text client protocol) and `KvClient` (follows leader redirects). |

About 1,800 lines of main code and 1,750 lines of tests.

## The test harness

The point of the project. Raft is notoriously easy to get *almost* right, so the tests try hard to break it.

**`JepsenRunner`** starts a 5-node cluster, runs 6 concurrent clients doing reads, writes and compare-and-set on 6 keys, and lets a **`Nemesis`** loose at random: network partitions into minority/majority halves, isolating the current leader, isolating a random node, a "bridge" topology (two groups that can only talk through one node), crashing nodes (often the leader) and restarting them from their durable state. The simulated network also drops, duplicates, delays and reorders messages. Then:

1. **Linearizability check.** Every operation is recorded with invoke/return timestamps. `LinearizabilityChecker` (Wing-Gong search with Lowe-style memoisation, checked per key) decides whether any valid sequential order exists that respects real time. Operations whose outcome is unknown, such as a request to a leader that got cut off, are handled the way Jepsen does: they may have taken effect at any later moment, or never.
2. **Safety monitors** running throughout: at most one leader per term, and every node applies the same command at every log index.
3. **Convergence:** after healing, all replicas must end with identical state.

**The checker is itself tested.** `checker-accepts-valid-histories` and `checker-rejects-invalid-histories` use hand-written histories with known answers (stale reads, values that revert, an indeterminate write that can't be un-done, impossible CAS results).

**Negative control.** `jepsen-negative-control-stale-reads-are-caught` deliberately serves reads from a node's local state, skipping the log. The checker must flag it, and it does, printing the operations that can't be ordered. A test suite that has never been seen to fail proves little.

**Mutation testing.** I injected four classic Raft bugs into a scratch copy and ran the Jepsen tests against each:

| Injected bug | Caught? |
|---|---|
| Grant votes without checking the candidate's log is up to date | Yes (1 of 2 seeds) |
| Leader commits without waiting for a majority | Yes (2 of 2 seeds) |
| Count replicas to commit entries from older terms (the paper's Figure 8 bug) | **No**, needs a very specific interleaving |
| Vote not persisted before replying | **No**, nearly equivalent in practice, since the vote is usually saved as a side effect of the term bump |

So the harness is a good net, not a proof. Randomised testing finds bugs probabilistically; use `Soak` to widen the net (below).

## Running it

Requires JDK 21+. No other dependencies.

```bash
./run-tests.sh                       # build + full suite (about 30 seconds)
./run-tests.sh leader-failover       # one test by name

# hunt for rare bugs: 50 seeds starting at 1000, 8 seconds each
java -cp out/main:out/test raftkv.Soak 1000 50 8000
```

Run a real 3-node cluster (three terminals):

```bash
CL=n1=127.0.0.1:7001:8001,n2=127.0.0.1:7002:8002,n3=127.0.0.1:7003:8003
java -cp out/main raftkv.KvServer --id n1 --data-dir data/n1 --cluster $CL
java -cp out/main raftkv.KvServer --id n2 --data-dir data/n2 --cluster $CL
java -cp out/main raftkv.KvServer --id n3 --data-dir data/n3 --cluster $CL
```

```bash
S=127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
java -cp out/main raftkv.KvClient --servers $S PUT language Java      # OK
java -cp out/main raftkv.KvClient --servers $S GET language           # OK Java
java -cp out/main raftkv.KvClient --servers $S CAS counter NIL 1      # OK  (NIL = key absent)
```

Kill the leader and keep issuing commands: a new leader is elected and your data is still there. Restart the dead node and it catches up from its own disk plus the leader.

## Design notes

* **Single-threaded protocol core.** All node state lives on one executor thread, so the Raft logic has no locks. Messages, client requests and timer ticks are queued onto it.
* **Persist, then send.** Handlers queue outgoing messages; `flush()` saves durable state first and only then sends them. This enforces the rule that a node must not answer an RPC before the term, vote or log it depends on is on disk.
* **Outcomes are three-valued.** A client request ends `OK`, `NOT_LEADER` (provably not applied, safe to retry), or `INDETERMINATE` (appended but the leader lost power or leadership before learning the outcome). Pretending the third case doesn't exist is how storage systems lose data, and it's the case the checker is built around.
* **Safety guards in the code.** A node refuses to truncate a committed log entry and halts instead (visible in the mutation test above).

## Known limitations

Deliberately out of scope, and what I'd do next:

* **No cluster membership changes** (joint consensus). The node set is fixed at startup.
* **Reads go through the log.** Correct, but slower than ReadIndex or leader leases.
* **No PreVote or leader stickiness**, so a node returning from a partition can force one unnecessary election.
* **Snapshots are sent as a single message**, not chunked, and each save rewrites the whole state file. A real system would use a write-ahead log and incremental snapshotting.
* **The TCP transport has no authentication or TLS.** Netty or gRPC would slot in behind the `Transport` interface.
* **The simulated network is not deterministic**, because it uses real threads and clocks. A failing seed usually, but not always, reproduces.

## Layout

```
src/main/java/raftkv/
  RaftNode.java        election, replication, snapshots (the core)
  KvStateMachine.java  the replicated map + client de-duplication
  Transport.java       the network abstraction;  TcpTransport.java is the real one
  Storage.java         MemoryStorage (tests), FileStorage (fsync + atomic rename + CRC)
  Codec.java           binary wire/disk format
  KvServer.java        runnable server;  KvClient.java is the CLI client
src/test/java/raftkv/
  LinearizabilityChecker.java   Wing-Gong + memoisation checker
  JepsenRunner.java, Nemesis.java, History.java
  SimNetwork.java, SimCluster.java   partitions, drops, duplicates; crash/restart
  AllTests.java, TcpClusterTest.java, Soak.java
```
