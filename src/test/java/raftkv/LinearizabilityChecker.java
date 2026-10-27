package raftkv;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import raftkv.History.Kind;
import raftkv.History.Op;
import raftkv.History.Outcome;

/**
 * Linearizability checker for a map of independent registers (read / write / compare-and-set).
 *
 * <p>Algorithm: Wing &amp; Gong's search with Lowe's memoisation. A history is linearizable iff the
 * operations can be put in a total order that (a) respects real time (if A returned before B was
 * invoked, A comes first) and (b) is a valid sequential execution of a register. We depth-first
 * search over "which operation takes effect next", caching (set of operations already placed,
 * register value) pairs we have already shown to be dead ends.
 *
 * <p>Registers are independent, so each key is checked on its own (this is sound for linearizability
 * and keeps the search small).
 *
 * <p>Operations of unknown outcome (INFO) are handled the Jepsen way: they may have taken effect at
 * any time after they were invoked, or never. A timed-out read is dropped (it changes nothing). A
 * timed-out write/CAS is an <em>optional</em> operation with an infinite response time.
 */
public final class LinearizabilityChecker {
    private LinearizabilityChecker() {
    }

    public enum Verdict { LINEARIZABLE, VIOLATION, UNKNOWN }

    public record Report(Verdict verdict, String key, String detail, long statesExplored) {
        @Override
        public String toString() {
            return verdict + (key == null ? "" : " (key " + key + ")") + " after exploring " + statesExplored
                    + " states" + (detail.isEmpty() ? "" : "\n" + detail);
        }
    }

    public static Report check(List<Op> history, long maxStatesPerKey) {
        Map<String, List<Op>> byKey = new LinkedHashMap<>();
        for (Op op : history) {
            byKey.computeIfAbsent(op.key, k -> new ArrayList<>()).add(op);
        }
        long total = 0;
        Report unknown = null;
        for (Map.Entry<String, List<Op>> e : byKey.entrySet()) {
            Report r = checkKey(e.getKey(), e.getValue(), maxStatesPerKey);
            total += r.statesExplored();
            if (r.verdict() == Verdict.VIOLATION) {
                return r;
            }
            if (r.verdict() == Verdict.UNKNOWN && unknown == null) {
                unknown = r;
            }
        }
        if (unknown != null) {
            return unknown;
        }
        return new Report(Verdict.LINEARIZABLE, null, "", total);
    }

    // ---------------------------------------------------------------------------------------

    private static final String INVALID = new String("<invalid>"); // identity-compared sentinel

    private record Key(BitSet done, String state) {
    }

    private static final class BudgetExceeded extends RuntimeException {
        private static final long serialVersionUID = 1L;

        BudgetExceeded() {
            super(null, null, false, false);
        }
    }

    private static final class Search {
        final List<Op> ops;
        final int n;
        final long[] invoke;
        final long[] ret;
        final boolean[] completed;
        final int totalCompleted;
        final long budget;
        final Set<Key> seen = new HashSet<>();
        long explored;
        int bestDepth = -1;
        BitSet bestDone;
        String bestState;

        Search(List<Op> ops, long budget) {
            this.ops = ops;
            this.n = ops.size();
            this.invoke = new long[n];
            this.ret = new long[n];
            this.completed = new boolean[n];
            this.budget = budget;
            int c = 0;
            for (int i = 0; i < n; i++) {
                Op o = ops.get(i);
                invoke[i] = o.invokeNs;
                completed[i] = o.outcome == Outcome.OK;
                ret[i] = completed[i] ? o.returnNs : Long.MAX_VALUE;
                if (completed[i]) {
                    c++;
                }
            }
            this.totalCompleted = c;
        }

        /** Apply op i to a register holding {@code state}; INVALID if it cannot take effect now. */
        String step(int i, String state) {
            Op o = ops.get(i);
            switch (o.kind) {
                case READ:
                    return Objects.equals(state, o.readValue) ? state : INVALID;
                case WRITE:
                    return o.arg1;
                case CAS:
                    if (completed[i] && !o.casSuccess) {
                        return Objects.equals(state, o.arg1) ? INVALID : state; // failed CAS: value must differ
                    }
                    // succeeded, or unknown-outcome (which only matters if it did succeed)
                    return Objects.equals(state, o.arg1) ? o.arg2 : INVALID;
                default:
                    throw new IllegalStateException();
            }
        }

        boolean dfs(BitSet done, String state, int doneCompleted) {
            if (doneCompleted == totalCompleted) {
                return true;
            }
            if (doneCompleted > bestDepth) {
                bestDepth = doneCompleted;
                bestDone = (BitSet) done.clone();
                bestState = state;
            }
            if (++explored > budget) {
                throw new BudgetExceeded();
            }
            if (!seen.add(new Key((BitSet) done.clone(), state))) {
                return false;
            }
            // Anything invoked at or after the earliest response among unplaced completed ops
            // cannot be linearized before that op.
            long minRet = Long.MAX_VALUE;
            for (int i = 0; i < n; i++) {
                if (!done.get(i) && completed[i] && ret[i] < minRet) {
                    minRet = ret[i];
                }
            }
            for (int i = 0; i < n; i++) {
                if (done.get(i)) {
                    continue;
                }
                if (invoke[i] >= minRet) {
                    break; // ops are sorted by invoke time
                }
                String next = step(i, state);
                if (next == INVALID) {
                    continue;
                }
                done.set(i);
                if (dfs(done, next, doneCompleted + (completed[i] ? 1 : 0))) {
                    return true;
                }
                done.clear(i);
            }
            return false;
        }
    }

    static Report checkKey(String key, List<Op> keyOps, long budget) {
        List<Op> ops = new ArrayList<>();
        for (Op o : keyOps) {
            if (o.outcome == Outcome.FAIL) {
                continue; // definitely had no effect
            }
            if (o.outcome == Outcome.INFO && o.kind == Kind.READ) {
                continue; // an unanswered read changes nothing
            }
            ops.add(o);
        }
        ops.sort(Comparator.comparingLong((Op o) -> o.invokeNs).thenComparingInt(o -> o.id));
        final Search s = new Search(ops, budget);

        // The recursion is as deep as the number of operations on the key; give it a big stack.
        final boolean[] result = new boolean[1];
        final boolean[] overBudget = new boolean[1];
        Thread t = new Thread(null, () -> {
            try {
                result[0] = s.dfs(new BitSet(s.n), null, 0);
            } catch (BudgetExceeded e) {
                overBudget[0] = true;
            }
        }, "lin-check", 512L * 1024 * 1024);
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }

        if (overBudget[0]) {
            return new Report(Verdict.UNKNOWN, key, "search budget of " + budget + " states exhausted", s.explored);
        }
        if (result[0]) {
            return new Report(Verdict.LINEARIZABLE, key, "", s.explored);
        }
        return new Report(Verdict.VIOLATION, key, explain(s), s.explored);
    }

    private static String explain(Search s) {
        StringBuilder sb = new StringBuilder();
        long base = Long.MAX_VALUE;
        for (Op o : s.ops) {
            base = Math.min(base, o.invokeNs);
        }
        sb.append("No valid linearization exists. The search managed to order ").append(s.bestDepth)
                .append(" of ").append(s.totalCompleted).append(" completed operations before every choice failed.\n");
        sb.append("Register value at the deepest point reached: ")
                .append(s.bestState == null ? "<absent>" : "\"" + s.bestState + "\"").append('\n');

        // The operations that could have gone next but could not be applied.
        long minRet = Long.MAX_VALUE;
        for (int i = 0; i < s.n; i++) {
            if (!s.bestDone.get(i) && s.completed[i] && s.ret[i] < minRet) {
                minRet = s.ret[i];
            }
        }
        sb.append("Operations that could not be placed next:\n");
        int shown = 0;
        for (int i = 0; i < s.n && shown < 8; i++) {
            if (s.bestDone.get(i) || s.invoke[i] >= minRet || !s.completed[i]) {
                continue;
            }
            Op o = s.ops.get(i);
            sb.append(String.format("  %-9s %s   invoked +%.1fms, returned +%.1fms%n", o.process, o,
                    (o.invokeNs - base) / 1e6, (o.returnNs - base) / 1e6));
            shown++;
        }
        return sb.toString().stripTrailing();
    }
}
