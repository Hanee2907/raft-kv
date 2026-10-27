package raftkv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A recorded concurrent history: every client operation with its invocation time, its response time
 * and what happened. Times come from one monotonic clock (System.nanoTime) in one JVM, so "A returned
 * before B was invoked" is a real happens-before fact.
 *
 * <p>Outcomes follow Jepsen's convention:
 * <ul>
 *   <li>OK: we saw the response.
 *   <li>FAIL: it definitely did not take effect (e.g. rejected before being appended).
 *   <li>INFO: we don't know (timeout / leadership lost). It may take effect at any later moment.
 * </ul>
 */
public final class History {

    public enum Kind { READ, WRITE, CAS }

    public enum Outcome { OK, FAIL, INFO }

    public static final class Op {
        public final int id;
        public final String process;
        public final Kind kind;
        public final String key;
        /** WRITE: value written. CAS: expected value (null = absent). READ: unused. */
        public final String arg1;
        /** CAS: new value. */
        public final String arg2;
        public long invokeNs;
        public long returnNs;
        public Outcome outcome = Outcome.INFO;
        /** READ result (null = key absent). */
        public String readValue;
        /** CAS result. */
        public boolean casSuccess;

        public Op(int id, String process, Kind kind, String key, String arg1, String arg2) {
            this.id = id;
            this.process = process;
            this.kind = kind;
            this.key = key;
            this.arg1 = arg1;
            this.arg2 = arg2;
        }

        @Override
        public String toString() {
            String what = switch (kind) {
                case READ -> "read " + key + (outcome == Outcome.OK ? " -> " + q(readValue) : "");
                case WRITE -> "write " + key + " = " + q(arg1);
                case CAS -> "cas " + key + " " + q(arg1) + " -> " + q(arg2)
                        + (outcome == Outcome.OK ? (casSuccess ? "  [succeeded]" : "  [failed]") : "");
            };
            return what + "  (" + outcome + ")";
        }

        private static String q(String s) {
            return s == null ? "<absent>" : "\"" + s + "\"";
        }
    }

    private final List<Op> ops = Collections.synchronizedList(new ArrayList<>());

    public Op invoke(String process, Kind kind, String key, String arg1, String arg2) {
        Op op = new Op(ops.size(), process, kind, key, arg1, arg2);
        op.invokeNs = System.nanoTime();
        ops.add(op);
        return op;
    }

    public void ok(Op op) {
        op.returnNs = System.nanoTime();
        op.outcome = Outcome.OK;
    }

    public void fail(Op op) {
        op.returnNs = System.nanoTime();
        op.outcome = Outcome.FAIL;
    }

    /** Outcome unknown: the response time stays "never". */
    public void info(Op op) {
        op.outcome = Outcome.INFO;
        op.returnNs = Long.MAX_VALUE;
    }

    public List<Op> ops() {
        synchronized (ops) {
            return new ArrayList<>(ops);
        }
    }

    public int count(Outcome o) {
        int n = 0;
        for (Op op : ops()) {
            if (op.outcome == o) {
                n++;
            }
        }
        return n;
    }
}
