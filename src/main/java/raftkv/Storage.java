package raftkv;

import java.io.IOException;
import java.util.List;

/**
 * Durable storage for everything Raft must not forget across a crash: current term, vote, the log
 * suffix, and the latest snapshot. {@code save} must be durable when it returns; RaftNode never
 * sends a message that depends on state before saving it.
 */
public interface Storage {

    record PersistentState(long term, String votedFor, long snapshotIndex, long snapshotTerm,
                           byte[] snapshot, List<LogEntry> log) {
        public static PersistentState empty() {
            return new PersistentState(0, null, 0, 0, null, List.of());
        }
    }

    PersistentState load() throws IOException;

    void save(PersistentState state) throws IOException;
}
