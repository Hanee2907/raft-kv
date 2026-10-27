package raftkv;

/**
 * Outcome of submitting a command to a node.
 *
 * <ul>
 *   <li>{@code OK}: the command was committed and applied; success/value come from the state machine.
 *   <li>{@code NOT_LEADER}: the node refused without appending anything, so the command definitely
 *       did NOT happen. {@code leaderHint} is the node's best guess at who the leader is.
 *   <li>{@code INDETERMINATE}: the command was appended but the node lost leadership (or the caller
 *       timed out) before learning whether it committed. It may or may not take effect later.
 * </ul>
 */
public record Result(Status status, boolean success, String value, String leaderHint) {

    public enum Status { OK, NOT_LEADER, INDETERMINATE }

    static Result ok(Reply r) {
        if (r == null) {
            return new Result(Status.OK, true, null, null);
        }
        return new Result(Status.OK, r.success(), r.value(), null);
    }

    public static Result notLeader(String hint) {
        return new Result(Status.NOT_LEADER, false, null, hint);
    }

    public static Result indeterminate() {
        return new Result(Status.INDETERMINATE, false, null, null);
    }
}
