package raftkv;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import raftkv.History.Kind;
import raftkv.History.Op;

/**
 * A Jepsen-style experiment: spin up a cluster, hammer it with concurrent clients while a nemesis
 * injects partitions and crashes, record every operation, then (1) check the history is linearizable,
 * (2) check the safety monitors stayed quiet, and (3) check the cluster converges once healed.
 *
 * <p>Client conventions (as in Jepsen): each client process runs one operation at a time. If an
 * operation's outcome is unknown (timeout, leader lost), that process is abandoned and the client
 * continues as a fresh process, because the old one conceptually still has a request in flight.
 */
public final class JepsenRunner {

    public static final class Config {
        public int nodes = 5;
        public int clients = 6;
        public int keys = 6;
        public long durationMs = 8_000;
        public long seed = 1;
        public boolean nemesis = true;
        /** Serve reads from a random node's local state (NOT linearizable). The checker must catch this. */
        public boolean unsafeLocalReads = false;
        public int snapshotThreshold = 40;
        public double dropRate = 0.01;
        public double dupRate = 0.02;
        public double slowRate = 0.02;
        public long opTimeoutMs = 700;
        public long checkerBudget = 3_000_000;
    }

    public static final class Outcome {
        public History history;
        public LinearizabilityChecker.Report report;
        public List<String> safetyViolations = new ArrayList<>();
        public List<String> nemesisLog = new ArrayList<>();
        public boolean converged;
        public String convergenceDetail = "";
        public int ok;
        public int fail;
        public int info;
        public long electedTerms;
        public long snapshotsTaken;
        public long snapshotsInstalled;

        public String summary() {
            return String.format("ops: %d ok, %d failed, %d indeterminate | terms with a leader: %d | snapshots taken/installed: %d/%d | checker: %s",
                    ok, fail, info, electedTerms, snapshotsTaken, snapshotsInstalled, report.verdict());
        }
    }

    public static Outcome run(Config c) throws Exception {
        RaftNode.Config raftCfg = new RaftNode.Config();
        raftCfg.electionTimeoutMinMs = 120;
        raftCfg.electionTimeoutMaxMs = 260;
        raftCfg.heartbeatMs = 30;
        raftCfg.snapshotThreshold = c.snapshotThreshold;

        Outcome out = new Outcome();
        History history = new History();
        out.history = history;

        try (SimCluster cluster = new SimCluster(c.nodes, raftCfg, c.seed)) {
            cluster.net().dropRate = c.dropRate;
            cluster.net().dupRate = c.dupRate;
            cluster.net().slowRate = c.slowRate;
            cluster.waitForLeader(5_000);

            AtomicBoolean running = new AtomicBoolean(true);
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < c.clients; i++) {
                final int clientNo = i;
                Thread t = new Thread(() -> clientLoop(cluster, history, c, clientNo, running), "client-" + i);
                threads.add(t);
                t.start();
            }
            Nemesis nemesis = new Nemesis(cluster, c.seed * 7919);
            Thread nt = new Thread(nemesis, "nemesis");
            if (c.nemesis) {
                nt.start();
            }

            Thread.sleep(c.durationMs);

            running.set(false);
            for (Thread t : threads) {
                t.join();
            }
            nemesis.stop();
            if (c.nemesis) {
                nt.join();
                nemesis.healEverything();
            }
            out.nemesisLog = new ArrayList<>(nemesis.actions());

            // Let the cluster settle, then read every key through the log so that the final state is
            // part of the history too (this forces any lingering "maybe happened" writes to be explained).
            cluster.waitForLeader(10_000);
            long seq = 0;
            for (int k = 0; k < c.keys; k++) {
                String key = "k" + k;
                Op op = history.invoke("final", Kind.READ, key, null, null);
                Result r = cluster.executeEventually(Command.get("final", ++seq, key), 15_000);
                op.readValue = r.value();
                history.ok(op);
            }

            // Convergence: every node must end up with the same state and the same applied index.
            long deadline = System.nanoTime() + 15_000L * 1_000_000L;
            while (System.nanoTime() < deadline) {
                if (statesAgree(cluster, out)) {
                    out.converged = true;
                    break;
                }
                Thread.sleep(50);
            }
            if (!out.converged && out.convergenceDetail.isEmpty()) {
                out.convergenceDetail = "timed out waiting for convergence";
            }

            out.safetyViolations = cluster.violations();
            out.electedTerms = cluster.electedTerms();
            for (String id : cluster.ids()) {
                RaftNode n = cluster.node(id);
                if (n != null) {
                    out.snapshotsTaken += n.snapshotsTaken();
                    out.snapshotsInstalled += n.snapshotsInstalled();
                }
            }
        }

        out.ok = history.count(History.Outcome.OK);
        out.fail = history.count(History.Outcome.FAIL);
        out.info = history.count(History.Outcome.INFO);
        out.report = LinearizabilityChecker.check(history.ops(), c.checkerBudget);
        return out;
    }

    private static boolean statesAgree(SimCluster cluster, Outcome out) throws Exception {
        Map<String, String> ref = null;
        long refApplied = -1;
        for (String id : cluster.ids()) {
            RaftNode n = cluster.node(id);
            if (n == null) {
                out.convergenceDetail = id + " is down";
                return false;
            }
            Map<String, String> st = n.stateView().get();
            long applied = n.status().lastApplied();
            if (ref == null) {
                ref = st;
                refApplied = applied;
            } else if (!ref.equals(st) || applied != refApplied) {
                out.convergenceDetail = id + " differs (lastApplied " + applied + " vs " + refApplied + ")";
                return false;
            }
        }
        return true;
    }

    // ---- client workload --------------------------------------------------------------------

    private static void clientLoop(SimCluster cluster, History history, Config c, int clientNo, AtomicBoolean running) {
        Random rnd = new Random(c.seed * 31 + clientNo);
        int incarnation = 0;
        String process = "c" + clientNo + "." + incarnation;
        long seq = 0;
        long counter = 0;
        java.util.Map<String, String> lastSeen = new java.util.HashMap<>();
        List<String> ids = cluster.ids();

        while (running.get()) {
            String key = "k" + rnd.nextInt(c.keys);
            int dice = rnd.nextInt(100);
            Op op;
            Command cmd;
            if (dice < 40) {
                op = history.invoke(process, Kind.READ, key, null, null);
                cmd = Command.get(process, ++seq, key);
            } else if (dice < 80) {
                String v = process + "#" + (++counter);
                op = history.invoke(process, Kind.WRITE, key, v, null);
                cmd = Command.put(process, ++seq, key, v);
            } else {
                String expected = lastSeen.get(key); // often right, sometimes stale: both outcomes get exercised
                String v = process + "#" + (++counter);
                op = history.invoke(process, Kind.CAS, key, expected, v);
                cmd = Command.cas(process, ++seq, key, expected, v);
            }

            boolean abandon = false;
            if (c.unsafeLocalReads && op.kind == Kind.READ) {
                RaftNode n = cluster.node(ids.get(rnd.nextInt(ids.size())));
                try {
                    if (n == null) {
                        throw new IllegalStateException("down");
                    }
                    op.readValue = n.localReadUnsafe(key).get();
                    history.ok(op);
                } catch (Exception e) {
                    history.fail(op); // reads have no effect, so "failed" is always accurate
                }
            } else {
                long deadline = System.nanoTime() + c.opTimeoutMs * 1_000_000L;
                String target = ids.get(rnd.nextInt(ids.size()));
                boolean done = false;
                while (!done) {
                    long remainingMs = (deadline - System.nanoTime()) / 1_000_000L;
                    if (remainingMs <= 0) {
                        history.fail(op); // every attempt was refused before being appended
                        break;
                    }
                    Result r = cluster.execute(target, cmd, remainingMs);
                    switch (r.status()) {
                        case OK -> {
                            if (op.kind == Kind.READ) {
                                op.readValue = r.value();
                            } else if (op.kind == Kind.CAS) {
                                op.casSuccess = r.success();
                            }
                            history.ok(op);
                            if (op.kind == Kind.READ) {
                                setOrRemove(lastSeen, key, r.value());
                            } else if (op.kind == Kind.WRITE) {
                                lastSeen.put(key, op.arg1);
                            } else {
                                setOrRemove(lastSeen, key, r.success() ? op.arg2 : r.value());
                            }
                            done = true;
                        }
                        case NOT_LEADER -> {
                            target = r.leaderHint() != null ? r.leaderHint() : ids.get(rnd.nextInt(ids.size()));
                            sleep(8);
                        }
                        case INDETERMINATE -> {
                            history.info(op);
                            abandon = true;
                            done = true;
                        }
                    }
                }
            }
            if (abandon) {
                process = "c" + clientNo + "." + (++incarnation);
                seq = 0;
            }
            sleep(rnd.nextInt(6));
        }
    }

    private static void setOrRemove(java.util.Map<String, String> m, String k, String v) {
        if (v == null) {
            m.remove(k);
        } else {
            m.put(k, v);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
