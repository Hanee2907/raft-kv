package raftkv;

import java.util.List;

/** "Disk" that survives a simulated node crash because the test harness keeps the object around. */
public final class MemoryStorage implements Storage {
    private volatile PersistentState state = PersistentState.empty();

    @Override
    public PersistentState load() {
        return state;
    }

    @Override
    public void save(PersistentState s) {
        // LogEntry is immutable, so a shallow copy of the list is a safe point-in-time copy.
        state = new PersistentState(s.term(), s.votedFor(), s.snapshotIndex(), s.snapshotTerm(),
                s.snapshot(), List.copyOf(s.log()));
    }
}
