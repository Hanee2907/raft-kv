package raftkv;

import java.util.Objects;

/** Tiny assertion helpers so the suite has no external dependencies. */
final class Check {
    private Check() {
    }

    interface Condition {
        boolean test() throws Exception;
    }

    static void that(boolean cond, String message) {
        if (!cond) {
            throw new AssertionError(message);
        }
    }

    static void eq(Object expected, Object actual, String message) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(message + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    /** Polls until the condition holds, or fails after the timeout. */
    static void eventually(String what, long timeoutMs, Condition cond) throws Exception {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        Exception lastError = null;
        while (System.nanoTime() < deadline) {
            try {
                if (cond.test()) {
                    return;
                }
            } catch (Exception e) {
                lastError = e;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("timed out after " + timeoutMs + "ms waiting for: " + what
                + (lastError == null ? "" : " (last error: " + lastError + ")"));
    }
}
