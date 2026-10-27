package raftkv;

/**
 * What the state machine returns for a command.
 * GET: success=true, value=current value (null if absent). PUT: success=true.
 * CAS: success=whether the swap happened, value=the value found before the swap attempt.
 */
public record Reply(boolean success, String value) {
}
