package raftkv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import raftkv.Messages.*;

/**
 * One Raft server: leader election, log replication, snapshots/log compaction, and a replicated
 * key-value state machine on top.
 *
 * <h2>Threading model</h2>
 * All protocol state is owned by a single thread (the node's executor). Inbound messages, client
 * submissions and timer ticks are all queued onto it, so the protocol code has no locks. Handlers
 * never send directly: they queue outgoing messages in an outbox, and {@link #flush()} first persists
 * any changed durable state and only then sends. That enforces Raft's rule that a node must not
 * answer an RPC (or ask for a vote) before the term/vote/log it depends on is on disk.
 */
public final class RaftNode implements AutoCloseable {

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    public static final class Config {
        public int electionTimeoutMinMs = 150;
        public int electionTimeoutMaxMs = 300;
        public int heartbeatMs = 40;
        /** Take a snapshot once this many entries have been applied since the last one. */
        public int snapshotThreshold = 1000;
        public int maxEntriesPerAppend = 256;
        public long randomSeed = System.nanoTime();

        public Config copy() {
            Config c = new Config();
            c.electionTimeoutMinMs = electionTimeoutMinMs;
            c.electionTimeoutMaxMs = electionTimeoutMaxMs;
            c.heartbeatMs = heartbeatMs;
            c.snapshotThreshold = snapshotThreshold;
            c.maxEntriesPerAppend = maxEntriesPerAppend;
            c.randomSeed = randomSeed;
            return c;
        }
    }

    /** Immutable point-in-time view, safe to read from any thread. */
    public record Status(String id, Role role, long term, String leaderId, long commitIndex,
                         long lastApplied, long snapshotIndex, int logSize) {
    }

    private record Pending(long term, CompletableFuture<Result> future) {
    }

    // ---- wiring -----------------------------------------------------------------------------
    private final String id;
    private final List<String> peers;
    private final int clusterSize;
    private final Transport transport;
    private final Storage storage;
    private final Config cfg;
    private final Random rng;
    private final ScheduledExecutorService exec;

    // ---- persistent state (saved before any dependent message is sent) ----------------------
    private long currentTerm;
    private String votedFor;
    private long snapshotIndex;     // last log index covered by the snapshot
    private long snapshotTerm;
    private byte[] snapshotData;    // null until the first snapshot
    private final ArrayList<LogEntry> log = new ArrayList<>(); // log.get(0) is index snapshotIndex+1
    private boolean dirty;

    // ---- volatile state ---------------------------------------------------------------------
    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private final KvStateMachine sm = new KvStateMachine();
    private long electionDeadline;  // System.nanoTime() based
    private long nextHeartbeat;

    // candidate
    private Set<String> votes = new HashSet<>();

    // leader
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<String, Long> snapshotSentAt = new HashMap<>();
    private final Map<Long, Pending> pending = new HashMap<>();

    private final List<Object[]> outbox = new ArrayList<>();

    // ---- observability (thread-safe) --------------------------------------------------------
    private volatile boolean closed;
    private volatile boolean started;
    private volatile Status status;
    private final AtomicLong snapshotsTaken = new AtomicLong();
    private final AtomicLong snapshotsInstalled = new AtomicLong();
    private volatile BiConsumer<String, Long> leaderListener;     // (nodeId, term) when elected
    private volatile BiConsumer<Long, Command> applyListener;     // (index, command) when applied

    public RaftNode(String id, List<String> allIds, Transport transport, Storage storage, Config cfg) {
        this.id = id;
        this.peers = allIds.stream().filter(x -> !x.equals(id)).toList();
        this.clusterSize = allIds.size();
        this.transport = transport;
        this.storage = storage;
        this.cfg = cfg;
        this.rng = new Random(cfg.randomSeed);
        this.exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-" + id);
            t.setDaemon(true);
            return t;
        });
        try {
            Storage.PersistentState ps = storage.load();
            currentTerm = ps.term();
            votedFor = ps.votedFor();
            snapshotIndex = ps.snapshotIndex();
            snapshotTerm = ps.snapshotTerm();
            snapshotData = ps.snapshot();
            log.addAll(ps.log());
        } catch (IOException e) {
            throw new IllegalStateException("cannot load persistent state for " + id, e);
        }
        if (snapshotData != null) {
            sm.restore(snapshotData);
        }
        commitIndex = snapshotIndex;
        lastApplied = snapshotIndex;
        resetElectionTimer();
        publish();
    }

    // =========================================================================================
    // Public API (any thread)
    // =========================================================================================

    public void setLeaderListener(BiConsumer<String, Long> l) {
        this.leaderListener = l;
    }

    public void setApplyListener(BiConsumer<Long, Command> l) {
        this.applyListener = l;
    }

    public void start() {
        if (started) {
            return;
        }
        started = true;
        transport.setReceiver((from, msg) -> event(() -> handle(from, msg)));
        exec.scheduleWithFixedDelay(() -> {
            try {
                if (!closed) {
                    tick();
                    flush();
                }
            } catch (Throwable t) {
                fatal(t);
            }
        }, 10, 10, TimeUnit.MILLISECONDS);
    }

    public String id() {
        return id;
    }

    public Status status() {
        return status;
    }

    public long snapshotsTaken() {
        return snapshotsTaken.get();
    }

    public long snapshotsInstalled() {
        return snapshotsInstalled.get();
    }

    /**
     * Proposes a command. The future completes with OK once the command is committed and applied,
     * NOT_LEADER immediately if this node is not the leader (nothing was appended), or INDETERMINATE
     * if leadership is lost before the outcome is known. It never completes if this node is cut off
     * but still believes it is leader, so callers should apply their own timeout.
     */
    public CompletableFuture<Result> submit(Command cmd) {
        CompletableFuture<Result> f = new CompletableFuture<>();
        try {
            exec.execute(() -> {
                try {
                    if (closed || role != Role.LEADER) {
                        f.complete(Result.notLeader(closed ? null : leaderId));
                        return;
                    }
                    log.add(new LogEntry(currentTerm, cmd));
                    dirty = true;
                    pending.put(lastLogIndex(), new Pending(currentTerm, f));
                    advanceCommit();   // a single-node cluster commits immediately
                    broadcastAppend(); // don't wait for the next heartbeat
                    flush();
                } catch (Throwable t) {
                    fatal(t);
                }
            });
        } catch (RejectedExecutionException e) {
            f.complete(Result.notLeader(null));
        }
        return f;
    }

    /** Copy of the state machine contents (test/diagnostic use). Completes with null if the node is down. */
    public CompletableFuture<Map<String, String>> stateView() {
        CompletableFuture<Map<String, String>> f = new CompletableFuture<>();
        try {
            exec.execute(() -> f.complete(closed ? null : sm.view()));
        } catch (RejectedExecutionException e) {
            f.complete(null);
        }
        return f;
    }

    /**
     * Reads straight from this node's local state machine WITHOUT going through the log or checking
     * leadership. Deliberately NOT linearizable; exists only so the test suite can prove that its
     * linearizability checker catches real violations.
     */
    public CompletableFuture<String> localReadUnsafe(String key) {
        CompletableFuture<String> f = new CompletableFuture<>();
        try {
            exec.execute(() -> {
                if (closed) {
                    f.completeExceptionally(new IllegalStateException("node down"));
                } else {
                    f.complete(sm.read(key));
                }
            });
        } catch (RejectedExecutionException e) {
            f.completeExceptionally(new IllegalStateException("node down"));
        }
        return f;
    }

    @Override
    public void close() {
        try {
            exec.execute(() -> {
                closed = true;
                failPending();
            });
        } catch (RejectedExecutionException ignored) {
            // already shut down
        }
        exec.shutdown();
        try {
            exec.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        closed = true;
        transport.setReceiver(null);
    }

    // =========================================================================================
    // Event loop plumbing (node thread only below this line)
    // =========================================================================================

    private void event(Runnable r) {
        try {
            exec.execute(() -> {
                if (closed) {
                    return;
                }
                try {
                    r.run();
                    flush();
                } catch (Throwable t) {
                    fatal(t);
                }
            });
        } catch (RejectedExecutionException ignored) {
            // node is shut down; drop the message like a dead machine would
        }
    }

    /** Persist (if needed), then send. Order matters: never send what we haven't made durable. */
    private void flush() {
        if (dirty) {
            try {
                storage.save(new Storage.PersistentState(currentTerm, votedFor, snapshotIndex,
                        snapshotTerm, snapshotData, log));
            } catch (IOException e) {
                outbox.clear();
                fatal(e); // fail-stop: a node that cannot persist must not keep participating
                return;
            }
            dirty = false;
        }
        if (!outbox.isEmpty()) {
            List<Object[]> out = new ArrayList<>(outbox);
            outbox.clear();
            for (Object[] o : out) {
                transport.send((String) o[0], (Message) o[1]);
            }
        }
        publish();
    }

    private void fatal(Throwable t) {
        System.err.println("[raft " + id + "] FATAL, stopping node: " + t);
        t.printStackTrace();
        closed = true;
        failPending();
    }

    private void publish() {
        status = new Status(id, role, currentTerm, leaderId, commitIndex, lastApplied, snapshotIndex, log.size());
    }

    private void send(String to, Message m) {
        outbox.add(new Object[]{to, m});
    }

    private static long ms(long millis) {
        return millis * 1_000_000L;
    }

    private void resetElectionTimer() {
        int span = Math.max(1, cfg.electionTimeoutMaxMs - cfg.electionTimeoutMinMs + 1);
        electionDeadline = System.nanoTime() + ms(cfg.electionTimeoutMinMs + rng.nextInt(span));
    }

    private void tick() {
        long now = System.nanoTime();
        if (role == Role.LEADER) {
            if (now - nextHeartbeat >= 0) {
                nextHeartbeat = now + ms(cfg.heartbeatMs);
                broadcastAppend();
            }
        } else if (now - electionDeadline >= 0) {
            startElection();
        }
    }

    // =========================================================================================
    // Log helpers
    // =========================================================================================

    private long lastLogIndex() {
        return snapshotIndex + log.size();
    }

    private long lastLogTerm() {
        return log.isEmpty() ? snapshotTerm : log.get(log.size() - 1).term();
    }

    /** Term of the entry at index, or -1 if unknown (compacted away or beyond the end). */
    private long termAt(long index) {
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        if (index < snapshotIndex || index > lastLogIndex()) {
            return -1;
        }
        return log.get((int) (index - snapshotIndex - 1)).term();
    }

    private LogEntry entryAt(long index) {
        return log.get((int) (index - snapshotIndex - 1));
    }

    private void truncateFrom(long index) {
        if (index <= commitIndex) {
            throw new IllegalStateException("safety violation: attempt to truncate committed entry " + index
                    + " (commitIndex=" + commitIndex + ")");
        }
        log.subList((int) (index - snapshotIndex - 1), log.size()).clear();
        dirty = true;
    }

    // =========================================================================================
    // Role transitions
    // =========================================================================================

    /** Adopt a newer term (if any) and become a follower. */
    private void stepDown(long term) {
        boolean wasLeader = role == Role.LEADER;
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            dirty = true;
        }
        role = Role.FOLLOWER;
        if (wasLeader) {
            failPending();
            resetElectionTimer();
        }
    }

    private void startElection() {
        currentTerm++;
        votedFor = id;
        role = Role.CANDIDATE;
        leaderId = null;
        votes = new HashSet<>();
        votes.add(id);
        dirty = true;
        resetElectionTimer();
        if (clusterSize == 1) {
            becomeLeader();
            return;
        }
        for (String p : peers) {
            send(p, new RequestVote(currentTerm, id, lastLogIndex(), lastLogTerm()));
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        long next = lastLogIndex() + 1;
        for (String p : peers) {
            nextIndex.put(p, next);
            matchIndex.put(p, 0L);
        }
        snapshotSentAt.clear();
        // A leader may only count replicas to commit entries from its OWN term (Raft paper, section 5.4.2).
        // The no-op gives it such an entry, which in turn commits everything before it.
        log.add(new LogEntry(currentTerm, Command.noop()));
        dirty = true;
        BiConsumer<String, Long> l = leaderListener;
        if (l != null) {
            l.accept(id, currentTerm);
        }
        nextHeartbeat = System.nanoTime() + ms(cfg.heartbeatMs);
        advanceCommit();
        broadcastAppend();
    }

    private void failPending() {
        List<Pending> all = new ArrayList<>(pending.values());
        pending.clear();
        for (Pending p : all) {
            p.future().complete(Result.indeterminate());
        }
    }

    // =========================================================================================
    // Message handling
    // =========================================================================================

    private void handle(String from, Message m) {
        if (m.term() > currentTerm) {
            stepDown(m.term());
        }
        switch (m) {
            case RequestVote rv -> onRequestVote(from, rv);
            case RequestVoteReply r -> onRequestVoteReply(from, r);
            case AppendEntries ae -> onAppendEntries(from, ae);
            case AppendEntriesReply r -> onAppendEntriesReply(from, r);
            case InstallSnapshot is -> onInstallSnapshot(from, is);
            case InstallSnapshotReply r -> onInstallSnapshotReply(from, r);
        }
    }

    private void onRequestVote(String from, RequestVote rv) {
        boolean grant = false;
        if (rv.term() == currentTerm) {
            boolean upToDate = rv.lastLogTerm() > lastLogTerm()
                    || (rv.lastLogTerm() == lastLogTerm() && rv.lastLogIndex() >= lastLogIndex());
            if (upToDate && (votedFor == null || votedFor.equals(rv.candidateId()))) {
                grant = true;
                votedFor = rv.candidateId();
                dirty = true;
                resetElectionTimer();
            }
        }
        send(from, new RequestVoteReply(currentTerm, grant));
    }

    private void onRequestVoteReply(String from, RequestVoteReply r) {
        if (role != Role.CANDIDATE || r.term() != currentTerm || !r.granted()) {
            return;
        }
        votes.add(from);
        if (votes.size() > clusterSize / 2) {
            becomeLeader();
        }
    }

    private void onAppendEntries(String from, AppendEntries ae) {
        if (ae.term() < currentTerm) {
            send(from, new AppendEntriesReply(currentTerm, false, 0, 0, -1));
            return;
        }
        // Same term: there is a legitimate leader. (A candidate in this term must yield.)
        role = Role.FOLLOWER;
        leaderId = ae.leaderId();
        resetElectionTimer();

        final long lastNew = ae.prevLogIndex() + ae.entries().size();
        long prevIdx = ae.prevLogIndex();
        long prevTerm = ae.prevLogTerm();
        List<LogEntry> entries = ae.entries();

        if (prevIdx < snapshotIndex) {
            // The leader is sending entries we have already compacted into our snapshot. Everything
            // up to snapshotIndex is committed, hence identical on the leader; skip that prefix.
            long skip = snapshotIndex - prevIdx;
            entries = skip >= entries.size() ? List.of() : entries.subList((int) skip, entries.size());
            prevIdx = snapshotIndex;
            prevTerm = snapshotTerm;
        }

        if (prevIdx > lastLogIndex()) {
            send(from, new AppendEntriesReply(currentTerm, false, 0, lastLogIndex() + 1, -1));
            return;
        }
        if (termAt(prevIdx) != prevTerm) {
            long conflictTerm = termAt(prevIdx);
            long conflictIndex = prevIdx;
            while (conflictIndex > snapshotIndex + 1 && termAt(conflictIndex - 1) == conflictTerm) {
                conflictIndex--;
            }
            send(from, new AppendEntriesReply(currentTerm, false, 0, conflictIndex, conflictTerm));
            return;
        }

        for (int k = 0; k < entries.size(); k++) {
            long idx = prevIdx + 1 + k;
            LogEntry e = entries.get(k);
            if (idx <= lastLogIndex()) {
                if (termAt(idx) == e.term()) {
                    continue; // already have it (duplicate or reordered message)
                }
                truncateFrom(idx); // conflict: drop our suffix, the leader's log wins
            }
            log.add(e);
            dirty = true;
        }

        long newCommit = Math.min(ae.leaderCommit(), lastNew);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
        }
        send(from, new AppendEntriesReply(currentTerm, true, lastNew, 0, -1));
    }

    private void onAppendEntriesReply(String from, AppendEntriesReply r) {
        if (role != Role.LEADER || r.term() != currentTerm) {
            return;
        }
        if (r.success()) {
            if (r.matchIndex() > matchIndex.get(from)) {
                matchIndex.put(from, r.matchIndex());
                nextIndex.put(from, r.matchIndex() + 1);
                advanceCommit();
                if (nextIndex.get(from) <= lastLogIndex()) {
                    sendAppend(from); // keep streaming until the follower is caught up
                }
            }
            return;
        }
        long newNext;
        if (r.conflictTerm() >= 0) {
            long found = -1;
            for (long i = lastLogIndex(); i > snapshotIndex; i--) {
                long t = termAt(i);
                if (t == r.conflictTerm()) {
                    found = i;
                    break;
                }
                if (t < r.conflictTerm()) {
                    break;
                }
            }
            newNext = found >= 0 ? found + 1 : r.conflictIndex();
        } else {
            newNext = r.conflictIndex();
        }
        newNext = Math.max(newNext, matchIndex.get(from) + 1);
        newNext = Math.min(newNext, lastLogIndex() + 1);
        newNext = Math.max(newNext, 1);
        if (newNext < nextIndex.get(from)) {
            nextIndex.put(from, newNext);
            sendAppend(from);
        }
    }

    private void onInstallSnapshot(String from, InstallSnapshot is) {
        if (is.term() < currentTerm) {
            send(from, new InstallSnapshotReply(currentTerm, 0));
            return;
        }
        role = Role.FOLLOWER;
        leaderId = is.leaderId();
        resetElectionTimer();

        long idx = is.lastIncludedIndex();
        if (idx <= commitIndex) {
            // We already have all of this; just acknowledge.
            send(from, new InstallSnapshotReply(currentTerm, idx));
            return;
        }
        if (idx <= lastLogIndex() && termAt(idx) == is.lastIncludedTerm()) {
            log.subList(0, (int) (idx - snapshotIndex)).clear(); // keep the suffix after the snapshot
        } else {
            log.clear();
        }
        snapshotIndex = idx;
        snapshotTerm = is.lastIncludedTerm();
        snapshotData = is.data();
        dirty = true;
        sm.restore(is.data());
        lastApplied = idx;
        commitIndex = idx;
        snapshotsInstalled.incrementAndGet();
        send(from, new InstallSnapshotReply(currentTerm, idx));
    }

    private void onInstallSnapshotReply(String from, InstallSnapshotReply r) {
        if (role != Role.LEADER || r.term() != currentTerm) {
            return;
        }
        if (r.lastIncludedIndex() > matchIndex.get(from)) {
            matchIndex.put(from, r.lastIncludedIndex());
            nextIndex.put(from, Math.max(nextIndex.get(from), r.lastIncludedIndex() + 1));
            advanceCommit();
            if (nextIndex.get(from) <= lastLogIndex()) {
                sendAppend(from);
            }
        }
    }

    // =========================================================================================
    // Replication, commit, apply, snapshot
    // =========================================================================================

    private void broadcastAppend() {
        for (String p : peers) {
            sendAppend(p);
        }
    }

    private void sendAppend(String peer) {
        long next = nextIndex.get(peer);
        if (next <= snapshotIndex) {
            // The entries this follower needs have been compacted away: ship the snapshot instead.
            long now = System.nanoTime();
            Long last = snapshotSentAt.get(peer);
            if (last != null && now - last < ms(cfg.heartbeatMs * 5L)) {
                return; // one is probably still in flight
            }
            snapshotSentAt.put(peer, now);
            send(peer, new InstallSnapshot(currentTerm, id, snapshotIndex, snapshotTerm, snapshotData));
            return;
        }
        long prev = next - 1;
        int from = (int) (next - snapshotIndex - 1);
        int to = Math.min(log.size(), from + cfg.maxEntriesPerAppend);
        // Must copy: subList is a live view of our log, which keeps changing.
        List<LogEntry> entries = from < to ? List.copyOf(log.subList(from, to)) : List.of();
        send(peer, new AppendEntries(currentTerm, id, prev, termAt(prev), entries, commitIndex));
    }

    private void advanceCommit() {
        for (long n = lastLogIndex(); n > commitIndex; n--) {
            if (termAt(n) != currentTerm) {
                break; // only entries from the current term commit by counting; older ones ride along
            }
            int count = 1; // ourselves
            for (String p : peers) {
                if (matchIndex.get(p) >= n) {
                    count++;
                }
            }
            if (count > clusterSize / 2) {
                commitIndex = n;
                break;
            }
        }
        applyCommitted();
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry e = entryAt(lastApplied);
            Reply reply = sm.apply(e.command());
            BiConsumer<Long, Command> al = applyListener;
            if (al != null) {
                al.accept(lastApplied, e.command());
            }
            Pending p = pending.remove(lastApplied);
            if (p != null) {
                // If a different entry committed at this index, ours was overwritten after a leader change.
                p.future().complete(p.term() == e.term() ? Result.ok(reply) : Result.indeterminate());
            }
        }
        if (lastApplied - snapshotIndex >= cfg.snapshotThreshold) {
            takeSnapshot();
        }
    }

    private void takeSnapshot() {
        long newIndex = lastApplied;
        long newTerm = termAt(newIndex);
        byte[] data = sm.snapshot();
        log.subList(0, (int) (newIndex - snapshotIndex)).clear();
        snapshotIndex = newIndex;
        snapshotTerm = newTerm;
        snapshotData = data;
        dirty = true;
        snapshotsTaken.incrementAndGet();
    }
}
