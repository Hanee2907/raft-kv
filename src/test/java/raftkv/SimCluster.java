package raftkv;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * N Raft nodes on a {@link SimNetwork}, with crash/restart (the "disk" survives, memory does not)
 * and continuous safety monitoring:
 * <ul>
 *   <li>election safety: at most one leader is ever elected per term;
 *   <li>state machine safety: every node applies the same command at the same log index, always.
 * </ul>
 * Violations are collected and surfaced by {@link #violations()}.
 */
public final class SimCluster implements AutoCloseable {
    private final List<String> ids = new ArrayList<>();
    private final SimNetwork net = new SimNetwork();
    private final Map<String, MemoryStorage> disks = new ConcurrentHashMap<>();
    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
    private final RaftNode.Config baseCfg;
    private final long seed;
    private final AtomicLong incarnation = new AtomicLong();

    private final Map<Long, Set<String>> leadersByTerm = new ConcurrentHashMap<>();
    private final Map<Long, Command> appliedAt = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
    private final AtomicLong appliedEvents = new AtomicLong();

    public SimCluster(int n, RaftNode.Config cfg, long seed) {
        this.baseCfg = cfg;
        this.seed = seed;
        for (int i = 1; i <= n; i++) {
            ids.add("n" + i);
        }
        for (String id : ids) {
            disks.put(id, new MemoryStorage());
        }
        for (String id : ids) {
            startNode(id);
        }
    }

    public List<String> ids() {
        return ids;
    }

    public SimNetwork net() {
        return net;
    }

    public RaftNode node(String id) {
        return nodes.get(id);
    }

    public boolean isUp(String id) {
        return nodes.containsKey(id);
    }

    public List<String> violations() {
        return new ArrayList<>(violations);
    }

    public long appliedEvents() {
        return appliedEvents.get();
    }

    public long electedTerms() {
        return leadersByTerm.size();
    }

    // ---- lifecycle --------------------------------------------------------------------------

    private void startNode(String id) {
        RaftNode.Config c = baseCfg.copy();
        c.randomSeed = seed * 1_000_003L + id.hashCode() * 31L + incarnation.incrementAndGet();
        RaftNode node = new RaftNode(id, ids, net.transportFor(id), disks.get(id), c);
        node.setLeaderListener((nodeId, term) -> {
            Set<String> set = leadersByTerm.computeIfAbsent(term, t -> ConcurrentHashMap.newKeySet());
            set.add(nodeId);
            if (set.size() > 1) {
                violations.add("ELECTION SAFETY: two leaders in term " + term + ": " + set);
            }
        });
        node.setApplyListener((index, cmd) -> {
            appliedEvents.incrementAndGet();
            Command prev = appliedAt.putIfAbsent(index, cmd);
            if (prev != null && !prev.equals(cmd)) {
                violations.add("STATE MACHINE SAFETY: index " + index + " applied as " + prev + " and as " + cmd
                        + " (seen on " + id + ")");
            }
        });
        nodes.put(id, node);
        node.start();
    }

    public void crash(String id) {
        RaftNode n = nodes.remove(id);
        if (n != null) {
            n.close();
        }
    }

    public void restart(String id) {
        if (!nodes.containsKey(id)) {
            startNode(id);
        }
    }

    public void restartAll() {
        for (String id : ids) {
            restart(id);
        }
    }

    public void crashAll() {
        for (String id : new ArrayList<>(nodes.keySet())) {
            crash(id);
        }
    }

    @Override
    public void close() {
        crashAll();
        net.close();
    }

    // ---- observation ------------------------------------------------------------------------

    /** The up node that believes it is leader in the highest term, or null. */
    public String leader() {
        String best = null;
        long bestTerm = -1;
        for (Map.Entry<String, RaftNode> e : nodes.entrySet()) {
            RaftNode.Status s = e.getValue().status();
            if (s.role() == RaftNode.Role.LEADER && s.term() > bestTerm) {
                best = e.getKey();
                bestTerm = s.term();
            }
        }
        return best;
    }

    public String waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            String l = leader();
            if (l != null) {
                return l;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("no leader elected within " + timeoutMs + "ms");
    }

    public Map<String, String> state(String id) throws Exception {
        RaftNode n = nodes.get(id);
        return n == null ? null : n.stateView().get(2, TimeUnit.SECONDS);
    }

    // ---- client operations ------------------------------------------------------------------

    /** One attempt against one node. A timeout is reported as INDETERMINATE (the command may still commit). */
    public Result execute(String nodeId, Command c, long timeoutMs) {
        RaftNode n = nodes.get(nodeId);
        if (n == null) {
            return Result.notLeader(null);
        }
        try {
            return n.submit(c).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return Result.indeterminate();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Result.indeterminate();
        } catch (ExecutionException e) {
            return Result.indeterminate();
        }
    }

    /** Retries (on any failure) until it succeeds. Only safe for idempotent commands such as PUT or GET. */
    public Result executeEventually(Command c, long totalTimeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + totalTimeoutMs * 1_000_000L;
        Result last = Result.notLeader(null);
        while (System.nanoTime() < deadline) {
            String target = leader();
            if (target == null) {
                Thread.sleep(10);
                continue;
            }
            last = execute(target, c, 1000);
            if (last.status() == Result.Status.OK) {
                return last;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("command did not commit within " + totalTimeoutMs + "ms; last result: " + last);
    }

    public void put(String key, String value) throws InterruptedException {
        executeEventually(Command.put("", 0, key, value), 10_000);
    }

    public String get(String key) throws InterruptedException {
        return executeEventually(Command.get("", 0, key), 10_000).value();
    }
}
