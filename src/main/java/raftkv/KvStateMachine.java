package raftkv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The replicated state: a string-to-string map, plus a per-client "last applied sequence number"
 * table so that a retried command is applied at most once. Not thread-safe: it is only touched from
 * the owning RaftNode's single thread.
 */
public final class KvStateMachine {
    private final HashMap<String, String> data = new HashMap<>();
    private final HashMap<String, Long> lastSeq = new HashMap<>();
    private final HashMap<String, Reply> lastReply = new HashMap<>();

    /** Applies one committed command. Returns null for NOOP. */
    public Reply apply(Command c) {
        if (c.type() == Command.Type.NOOP) {
            return null;
        }
        String cid = c.clientId();
        boolean tracked = cid != null && !cid.isEmpty();
        if (tracked) {
            Long last = lastSeq.get(cid);
            if (last != null && c.seq() <= last) {
                // Duplicate of the last request: replay its answer. Older than that: client has moved on.
                return c.seq() == last ? lastReply.get(cid) : new Reply(false, null);
            }
        }
        Reply r = switch (c.type()) {
            case GET -> new Reply(true, data.get(c.key()));
            case PUT -> {
                data.put(c.key(), c.value());
                yield new Reply(true, null);
            }
            case CAS -> {
                String cur = data.get(c.key());
                if (Objects.equals(cur, c.expected())) {
                    data.put(c.key(), c.value());
                    yield new Reply(true, cur);
                }
                yield new Reply(false, cur);
            }
            case NOOP -> null;
        };
        if (tracked) {
            lastSeq.put(cid, c.seq());
            lastReply.put(cid, r);
        }
        return r;
    }

    /** Read without going through the log. Only for tests and for the deliberately-unsafe negative control. */
    public String read(String key) {
        return data.get(key);
    }

    public Map<String, String> view() {
        return new HashMap<>(data);
    }

    public byte[] snapshot() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);
            out.writeInt(data.size());
            for (Map.Entry<String, String> e : data.entrySet()) {
                Codec.writeString(out, e.getKey());
                Codec.writeString(out, e.getValue());
            }
            out.writeInt(lastSeq.size());
            for (Map.Entry<String, Long> e : lastSeq.entrySet()) {
                Codec.writeString(out, e.getKey());
                out.writeLong(e.getValue());
                Reply r = lastReply.get(e.getKey());
                out.writeBoolean(r != null);
                if (r != null) {
                    out.writeBoolean(r.success());
                    Codec.writeString(out, r.value());
                }
            }
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void restore(byte[] bytes) {
        data.clear();
        lastSeq.clear();
        lastReply.clear();
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            int n = in.readInt();
            for (int i = 0; i < n; i++) {
                String k = Codec.readString(in);
                data.put(k, Codec.readString(in));
            }
            int m = in.readInt();
            for (int i = 0; i < m; i++) {
                String cid = Codec.readString(in);
                lastSeq.put(cid, in.readLong());
                if (in.readBoolean()) {
                    boolean ok = in.readBoolean();
                    lastReply.put(cid, new Reply(ok, Codec.readString(in)));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("corrupt snapshot", e);
        }
    }
}
