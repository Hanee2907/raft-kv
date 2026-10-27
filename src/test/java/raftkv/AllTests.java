package raftkv;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import raftkv.History.Kind;
import raftkv.History.Op;
import raftkv.History.Outcome;
import raftkv.LinearizabilityChecker.Verdict;
import raftkv.Messages.*;

/**
 * The whole test suite. Run with no arguments for everything, or pass test names to run a subset.
 * Exits non-zero if anything fails.
 */
public final class AllTests {

    interface TestBody {
        void run() throws Exception;
    }

    private static final Map<String, TestBody> TESTS = new LinkedHashMap<>();

    static {
        // --- building blocks
        TESTS.put("codec-roundtrip", AllTests::codecRoundTrip);
        TESTS.put("file-storage", AllTests::fileStorage);
        TESTS.put("state-machine-dedup-and-snapshot", AllTests::stateMachine);
        // --- the checker itself must be right before we trust it
        TESTS.put("checker-accepts-valid-histories", AllTests::checkerAccepts);
        TESTS.put("checker-rejects-invalid-histories", AllTests::checkerRejects);
        // --- raft protocol
        TESTS.put("single-node-cluster", AllTests::singleNode);
        TESTS.put("elects-exactly-one-leader", AllTests::electsOneLeader);
        TESTS.put("leader-failover", AllTests::leaderFailover);
        TESTS.put("follower-catches-up-after-partition", AllTests::followerCatchUp);
        TESTS.put("isolated-leader-writes-are-discarded", AllTests::isolatedLeaderWritesDiscarded);
        TESTS.put("snapshot-compaction-and-install", AllTests::snapshots);
        TESTS.put("full-cluster-restart-keeps-data", AllTests::fullRestart);
        // --- the headline: partitions + crashes + linearizability
        TESTS.put("jepsen-partitions-and-crashes-seed-1", () -> jepsen(1));
        TESTS.put("jepsen-partitions-and-crashes-seed-2", () -> jepsen(2));
        TESTS.put("jepsen-negative-control-stale-reads-are-caught", AllTests::negativeControl);
        // --- real sockets + real disk
        TESTS.put("tcp-cluster-with-file-storage", TcpClusterTest::run);
    }

    public static void main(String[] args) {
        List<String> wanted = Arrays.asList(args);
        int passed = 0;
        List<String> failed = new ArrayList<>();
        for (Map.Entry<String, TestBody> e : TESTS.entrySet()) {
            if (!wanted.isEmpty() && !wanted.contains(e.getKey())) {
                continue;
            }
            long t0 = System.nanoTime();
            try {
                e.getValue().run();
                System.out.printf("PASS  %-52s %5.1fs%n", e.getKey(), (System.nanoTime() - t0) / 1e9);
                passed++;
            } catch (Throwable t) {
                System.out.printf("FAIL  %-52s %5.1fs%n", e.getKey(), (System.nanoTime() - t0) / 1e9);
                System.out.println("      " + String.valueOf(t).replace("\n", "\n      "));
                if (!(t instanceof AssertionError)) {
                    t.printStackTrace(System.out);
                }
                failed.add(e.getKey());
            }
        }
        System.out.println();
        System.out.println(passed + " passed, " + failed.size() + " failed");
        System.exit(failed.isEmpty() ? 0 : 1);
    }

    // =========================================================================================
    // Building blocks
    // =========================================================================================

    private static void codecRoundTrip() throws Exception {
        List<LogEntry> entries = List.of(
                new LogEntry(3, Command.noop()),
                new LogEntry(3, Command.put("c1", 7, "k", "v é中")),
                new LogEntry(4, Command.cas("c1", 8, "k", null, "new")),
                new LogEntry(4, Command.get("", 0, "k")));
        List<Message> msgs = List.of(
                new RequestVote(5, "n1", 10, 4),
                new RequestVoteReply(5, true),
                new AppendEntries(5, "n1", 9, 4, entries, 8),
                new AppendEntries(5, "n1", 9, 4, List.of(), 8),
                new AppendEntriesReply(5, false, 0, 3, 2),
                new InstallSnapshotReply(5, 99));
        for (Message m : msgs) {
            Check.eq(m, Codec.decode(Codec.encode(m)), "round trip of " + m);
        }
        byte[] data = {1, 2, 3, 0, -1};
        InstallSnapshot is = (InstallSnapshot) Codec.decode(Codec.encode(new InstallSnapshot(6, "n2", 50, 5, data)));
        Check.that(is.term() == 6 && is.lastIncludedIndex() == 50 && Arrays.equals(is.data(), data), "snapshot message");

        // malformed input must fail cleanly, never allocate wildly or hang
        try {
            Codec.decode(new byte[]{3, 0, 0, 0, 0, 0, 0, 0, 1, 127, 127, 127, 127});
            Check.that(false, "garbage should not decode");
        } catch (java.io.IOException expected) {
            // good
        }
    }

    private static void fileStorage() throws Exception {
        Path dir = Files.createTempDirectory("raftkv-fs");
        try {
            FileStorage fs = new FileStorage(dir);
            Check.eq(0L, fs.load().term(), "fresh storage is empty");
            Storage.PersistentState s = new Storage.PersistentState(9, "n3", 20, 7, new byte[]{9, 8, 7},
                    List.of(new LogEntry(8, Command.put("c", 1, "a", "b")), new LogEntry(9, Command.noop())));
            fs.save(s);
            Storage.PersistentState back = new FileStorage(dir).load();
            Check.eq(9L, back.term(), "term");
            Check.eq("n3", back.votedFor(), "votedFor");
            Check.eq(20L, back.snapshotIndex(), "snapshotIndex");
            Check.that(Arrays.equals(new byte[]{9, 8, 7}, back.snapshot()), "snapshot bytes");
            Check.eq(s.log(), back.log(), "log");

            // flip one byte: the checksum must catch it
            Path f = dir.resolve("raft-state.bin");
            byte[] bytes = Files.readAllBytes(f);
            bytes[bytes.length / 2] ^= 0x55;
            Files.write(f, bytes);
            try {
                new FileStorage(dir).load();
                Check.that(false, "corruption should be detected");
            } catch (java.io.IOException expected) {
                // good
            }
        } finally {
            deleteRecursively(dir);
        }
    }

    private static void stateMachine() {
        KvStateMachine sm = new KvStateMachine();
        Check.eq(true, sm.apply(Command.put("c1", 1, "x", "1")).success(), "put");
        // a retry of the same request must not be applied twice
        sm.apply(Command.put("c1", 2, "x", "2"));
        Reply dup = sm.apply(Command.put("c1", 2, "x", "SHOULD-NOT-APPLY"));
        Check.eq(true, dup.success(), "duplicate replays the old answer");
        Check.eq("2", sm.read("x"), "duplicate was not re-applied");
        // CAS semantics
        Check.eq(false, sm.apply(Command.cas("c2", 1, "x", "nope", "3")).success(), "cas with wrong expected");
        Check.eq(true, sm.apply(Command.cas("c2", 2, "x", "2", "3")).success(), "cas with right expected");
        Check.eq(true, sm.apply(Command.cas("c2", 3, "fresh", null, "v")).success(), "cas on absent key");
        // snapshot round trip keeps data AND the dedup table
        KvStateMachine copy = new KvStateMachine();
        copy.restore(sm.snapshot());
        Check.eq(sm.view(), copy.view(), "data survives snapshot");
        copy.apply(Command.put("c1", 2, "x", "SHOULD-NOT-APPLY"));
        Check.eq("3", copy.read("x"), "dedup table survives snapshot");
    }

    // =========================================================================================
    // The checker, tested against hand-written histories with known answers
    // =========================================================================================

    private static int nextId = 0;

    private static Op op(String proc, Kind kind, String key, String a1, String a2, long inv, long ret, Outcome out,
                         String readValue, boolean casOk) {
        Op o = new Op(nextId++, proc, kind, key, a1, a2);
        o.invokeNs = inv;
        o.returnNs = out == Outcome.INFO ? Long.MAX_VALUE : ret;
        o.outcome = out;
        o.readValue = readValue;
        o.casSuccess = casOk;
        return o;
    }

    private static Op w(String v, long inv, long ret) {
        return op("p", Kind.WRITE, "x", v, null, inv, ret, Outcome.OK, null, false);
    }

    private static Op wInfo(String v, long inv) {
        return op("q", Kind.WRITE, "x", v, null, inv, 0, Outcome.INFO, null, false);
    }

    private static Op r(String v, long inv, long ret) {
        return op("p", Kind.READ, "x", null, null, inv, ret, Outcome.OK, v, false);
    }

    private static Op cas(String exp, String nw, boolean success, long inv, long ret) {
        return op("p", Kind.CAS, "x", exp, nw, inv, ret, Outcome.OK, null, success);
    }

    private static Verdict verdict(Op... ops) {
        return LinearizabilityChecker.check(List.of(ops), 1_000_000).verdict();
    }

    private static void checkerAccepts() {
        Check.eq(Verdict.LINEARIZABLE, verdict(w("1", 0, 10), r("1", 20, 30)), "write then read");
        Check.eq(Verdict.LINEARIZABLE, verdict(r(null, 0, 10)), "read of untouched key is absent");
        Check.eq(Verdict.LINEARIZABLE, verdict(w("1", 0, 100), r(null, 10, 20), r("1", 30, 40)),
                "a read overlapping a write may see either value");
        Check.eq(Verdict.LINEARIZABLE, verdict(wInfo("1", 0), r("1", 50, 60)),
                "an indeterminate write can explain a later read");
        Check.eq(Verdict.LINEARIZABLE, verdict(wInfo("1", 0), r(null, 50, 60)),
                "an indeterminate write may also never have happened");
        Check.eq(Verdict.LINEARIZABLE, verdict(w("1", 0, 10), cas("1", "2", true, 20, 30), r("2", 40, 50)),
                "successful cas");
        Check.eq(Verdict.LINEARIZABLE, verdict(w("1", 0, 10), cas("5", "2", false, 20, 30), r("1", 40, 50)),
                "failed cas leaves value alone");
        Check.eq(Verdict.LINEARIZABLE, verdict(w("1", 0, 100), w("2", 0, 100), r("1", 110, 120), r("1", 130, 140)),
                "concurrent writes may land in either order");
        Check.eq(Verdict.LINEARIZABLE,
                verdict(op("p", Kind.WRITE, "x", "1", null, 0, 0, Outcome.FAIL, null, false), r(null, 20, 30)),
                "a definitely-failed write is ignored");
    }

    private static void checkerRejects() {
        Check.eq(Verdict.VIOLATION, verdict(w("1", 0, 10), r(null, 20, 30)),
                "stale read after a completed write");
        Check.eq(Verdict.VIOLATION, verdict(r("ghost", 0, 10)), "read of a value nobody wrote");
        Check.eq(Verdict.VIOLATION, verdict(w("1", 0, 100), w("2", 0, 100), r("2", 110, 120), r("1", 130, 140)),
                "value cannot revert with no further writes (non-monotonic reads)");
        Check.eq(Verdict.VIOLATION, verdict(w("1", 0, 10), wInfo("2", 20), r("2", 30, 40), r("1", 50, 60)),
                "an indeterminate write cannot be un-done");
        Check.eq(Verdict.VIOLATION, verdict(w("1", 0, 10), cas("5", "2", true, 20, 30)),
                "cas cannot succeed against the wrong value");
        Check.eq(Verdict.VIOLATION, verdict(w("1", 0, 10), cas("1", "2", false, 20, 30)),
                "cas cannot fail when the value matched");
        Check.eq(Verdict.VIOLATION, verdict(w("1", 0, 10), w("2", 20, 30), r("1", 40, 50)),
                "reading an overwritten value");
    }

    // =========================================================================================
    // Raft protocol tests
    // =========================================================================================

    private static RaftNode.Config fastConfig(int snapshotThreshold) {
        RaftNode.Config c = new RaftNode.Config();
        c.electionTimeoutMinMs = 120;
        c.electionTimeoutMaxMs = 260;
        c.heartbeatMs = 30;
        c.snapshotThreshold = snapshotThreshold;
        return c;
    }

    private static void noViolations(SimCluster c) {
        Check.that(c.violations().isEmpty(), "safety violations: " + c.violations());
    }

    private static void singleNode() throws Exception {
        try (SimCluster c = new SimCluster(1, fastConfig(1000), 1)) {
            c.waitForLeader(3000);
            c.put("a", "1");
            Check.eq("1", c.get("a"), "get after put");
            noViolations(c);
        }
    }

    private static void electsOneLeader() throws Exception {
        try (SimCluster c = new SimCluster(3, fastConfig(1000), 2)) {
            String leader = c.waitForLeader(5000);
            Thread.sleep(600); // stays stable
            int leaders = 0;
            for (String id : c.ids()) {
                RaftNode.Status s = c.node(id).status();
                if (s.role() == RaftNode.Role.LEADER) {
                    leaders++;
                }
            }
            Check.eq(1, leaders, "number of leaders");
            Check.eq(leader, c.leader(), "leader is stable without faults");
            Check.eventually("followers learn who the leader is", 3000, () -> {
                for (String id : c.ids()) {
                    if (!leader.equals(c.node(id).status().leaderId())) {
                        return false;
                    }
                }
                return true;
            });
            noViolations(c);
        }
    }

    private static void leaderFailover() throws Exception {
        try (SimCluster c = new SimCluster(3, fastConfig(1000), 3)) {
            String old = c.waitForLeader(5000);
            c.put("a", "1");
            long oldTerm = c.node(old).status().term();
            c.crash(old);

            Check.eventually("a new leader emerges", 5000, () -> c.leader() != null && !c.leader().equals(old));
            String fresh = c.leader();
            Check.that(c.node(fresh).status().term() > oldTerm, "new leader has a higher term");
            Check.eq("1", c.get("a"), "committed data survives failover");
            c.put("b", "2");

            c.restart(old);
            Check.eventually("restarted node rejoins as follower with full state", 5000, () -> {
                RaftNode n = c.node(old);
                return n.status().role() == RaftNode.Role.FOLLOWER && "2".equals(c.state(old).get("b"));
            });
            noViolations(c);
        }
    }

    private static void followerCatchUp() throws Exception {
        try (SimCluster c = new SimCluster(3, fastConfig(1000), 4)) {
            String leader = c.waitForLeader(5000);
            String lagging = c.ids().stream().filter(i -> !i.equals(leader)).findFirst().orElseThrow();
            c.net().isolate(lagging, c.ids());
            for (int i = 0; i < 60; i++) {
                c.put("k" + i, "v" + i); // majority of 2 still commits
            }
            Check.eq(null, c.state(lagging).get("k59"), "isolated follower is behind");
            c.net().heal();
            Check.eventually("follower catches up", 5000, () -> c.state(lagging).equals(c.state(leader)));
            Check.eq("v59", c.state(lagging).get("k59"), "caught up");
            noViolations(c);
        }
    }

    private static void isolatedLeaderWritesDiscarded() throws Exception {
        try (SimCluster c = new SimCluster(5, fastConfig(1000), 5)) {
            String old = c.waitForLeader(5000);
            c.put("before", "ok");

            c.net().isolate(old, c.ids());
            // The stranded leader still believes it leads. It accepts the write but can never commit it.
            Result stranded = c.execute(old, Command.put("", 0, "lost", "x"), 600);
            Check.eq(Result.Status.INDETERMINATE, stranded.status(), "write to a stranded leader never completes");

            Check.eventually("majority side elects a new leader", 5000, () -> {
                String l = c.leader();
                return l != null && !l.equals(old);
            });
            c.put("kept", "y");

            c.net().heal();
            Check.eventually("everyone converges", 8000, () -> {
                Map<String, String> ref = c.state(c.leader());
                for (String id : c.ids()) {
                    if (!ref.equals(c.state(id))) {
                        return false;
                    }
                }
                return true;
            });
            Map<String, String> st = c.state(old);
            Check.eq("y", st.get("kept"), "write committed by the majority survives");
            Check.eq(null, st.get("lost"), "write that only the stranded leader saw is discarded");
            Check.eq(RaftNode.Role.FOLLOWER, c.node(old).status().role(), "old leader stepped down");
            noViolations(c);
        }
    }

    private static void snapshots() throws Exception {
        try (SimCluster c = new SimCluster(3, fastConfig(20), 6)) {
            String leader = c.waitForLeader(5000);
            String lagging = c.ids().stream().filter(i -> !i.equals(leader)).findFirst().orElseThrow();
            c.net().isolate(lagging, c.ids());
            for (int i = 0; i < 100; i++) {
                c.put("k" + (i % 10), "v" + i);
            }
            RaftNode.Status ls = c.node(c.leader()).status();
            Check.that(c.node(c.leader()).snapshotsTaken() > 0, "leader took snapshots");
            Check.that(ls.snapshotIndex() > 0 && ls.logSize() < 60, "log was compacted: " + ls);

            // The lagging follower needs entries the leader no longer has, so it must be sent a snapshot.
            c.net().heal();
            Check.eventually("lagging follower catches up", 8000, () -> c.state(lagging).equals(c.state(c.leader())));
            Check.that(c.node(lagging).snapshotsInstalled() >= 1, "follower installed a snapshot rather than replaying the log");

            // A node that restarts after compaction recovers from snapshot + log suffix.
            c.crash(lagging);
            c.put("after", "crash");
            c.restart(lagging);
            Check.eventually("restarted node recovers", 8000, () -> c.state(lagging).equals(c.state(c.leader())));
            noViolations(c);
        }
    }

    private static void fullRestart() throws Exception {
        try (SimCluster c = new SimCluster(3, fastConfig(25), 7)) {
            c.waitForLeader(5000);
            for (int i = 0; i < 60; i++) {
                c.put("k" + i, "v" + i);
            }
            long termBefore = c.node(c.leader()).status().term();
            c.crashAll();
            c.restartAll();
            c.waitForLeader(8000);
            for (int i = 0; i < 60; i++) {
                Check.eq("v" + i, c.get("k" + i), "key k" + i + " after total power loss");
            }
            Check.that(c.node(c.leader()).status().term() >= termBefore, "term never goes backwards");
            noViolations(c);
        }
    }

    // =========================================================================================
    // Jepsen-style
    // =========================================================================================

    private static void jepsen(long seed) throws Exception {
        JepsenRunner.Config cfg = new JepsenRunner.Config();
        cfg.seed = seed;
        JepsenRunner.Outcome o = JepsenRunner.run(cfg);
        System.out.println("      " + o.summary());
        System.out.println("      nemesis: " + o.nemesisLog.size() + " actions, e.g. "
                + o.nemesisLog.subList(0, Math.min(3, o.nemesisLog.size())));
        Check.that(o.safetyViolations.isEmpty(), "safety violations: " + o.safetyViolations);
        Check.that(o.converged, "cluster did not converge after healing: " + o.convergenceDetail);
        Check.that(o.ok >= 100, "only " + o.ok + " operations succeeded; the run was too unproductive to mean anything");
        Check.that(o.info >= 1, "no indeterminate operations: the nemesis never actually hurt anything");
        Check.that(o.report.verdict() != Verdict.UNKNOWN, "checker gave up: " + o.report);
        Check.that(o.report.verdict() == Verdict.LINEARIZABLE, "NOT LINEARIZABLE:\n" + o.report
                + "\nnemesis log:\n" + String.join("\n", o.nemesisLog));
    }

    private static void negativeControl() throws Exception {
        JepsenRunner.Config cfg = new JepsenRunner.Config();
        cfg.seed = 3;
        cfg.nemesis = false;
        cfg.durationMs = 3_000;
        cfg.unsafeLocalReads = true;
        JepsenRunner.Outcome o = JepsenRunner.run(cfg);
        System.out.println("      " + o.summary());
        Check.that(o.report.verdict() == Verdict.VIOLATION,
                "reading from a follower's local state is NOT linearizable, so the checker must flag it. Got: " + o.report);
        System.out.println("      checker output for the intentionally broken reads:");
        System.out.println("      " + o.report.toString().replace("\n", "\n      "));
    }

    // =========================================================================================

    static void deleteRecursively(Path dir) {
        try (Stream<Path> s = Files.walk(dir)) {
            s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (Exception ignored) {
            // best effort
        }
    }
}
