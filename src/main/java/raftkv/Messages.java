package raftkv;

import java.util.List;

/** The six Raft RPC messages (requests and replies are all one-way messages on the wire). */
public final class Messages {
    private Messages() {
    }

    public sealed interface Message permits RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply,
            InstallSnapshot, InstallSnapshotReply {
        long term();
    }

    public record RequestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) implements Message {
    }

    public record RequestVoteReply(long term, boolean granted) implements Message {
    }

    public record AppendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                List<LogEntry> entries, long leaderCommit) implements Message {
    }

    /**
     * On failure, conflictIndex/conflictTerm let the leader skip back a whole term at a time instead
     * of one entry per round trip. On success, matchIndex is the last index known to match.
     */
    public record AppendEntriesReply(long term, boolean success, long matchIndex,
                                     long conflictIndex, long conflictTerm) implements Message {
    }

    /** The whole snapshot travels in one message (no chunking). */
    public record InstallSnapshot(long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm,
                                  byte[] data) implements Message {
    }

    public record InstallSnapshotReply(long term, long lastIncludedIndex) implements Message {
    }
}
