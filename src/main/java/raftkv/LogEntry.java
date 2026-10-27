package raftkv;

/** One slot of the replicated log: the term in which the leader received it, plus the command. */
public record LogEntry(long term, Command command) {
}
