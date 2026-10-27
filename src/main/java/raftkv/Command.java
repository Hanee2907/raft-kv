package raftkv;

/**
 * A client command replicated through the Raft log.
 *
 * <p>{@code clientId} + {@code seq} give exactly-once semantics for retried requests: the state
 * machine remembers the last sequence number it applied per client. An empty clientId disables
 * de-duplication.
 */
public record Command(Type type, String clientId, long seq, String key, String value, String expected) {

    public enum Type { NOOP, GET, PUT, CAS }

    private static final Command NOOP = new Command(Type.NOOP, "", 0, null, null, null);

    /** Appended by every new leader so that entries from earlier terms can be committed. */
    public static Command noop() {
        return NOOP;
    }

    public static Command get(String clientId, long seq, String key) {
        return new Command(Type.GET, clientId, seq, key, null, null);
    }

    public static Command put(String clientId, long seq, String key, String value) {
        return new Command(Type.PUT, clientId, seq, key, value, null);
    }

    /** Compare-and-set. {@code expected == null} means "the key must be absent". */
    public static Command cas(String clientId, long seq, String key, String expected, String value) {
        return new Command(Type.CAS, clientId, seq, key, value, expected);
    }
}
