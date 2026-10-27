package raftkv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import raftkv.Messages.*;

/**
 * Hand-written binary encoding for messages, log entries and persistent state. Deliberately avoids
 * Java serialization (which is unsafe to run on network input). Every length is bounds-checked.
 */
public final class Codec {
    private Codec() {
    }

    static final int MAX_BLOB = 256 * 1024 * 1024;

    // ---- primitives -------------------------------------------------------------------------

    public static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
            return;
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    public static String readString(DataInputStream in) throws IOException {
        byte[] b = readBytes(in);
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    public static void writeBytes(DataOutputStream out, byte[] b) throws IOException {
        if (b == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(b.length);
        out.write(b);
    }

    public static byte[] readBytes(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0) {
            return null;
        }
        if (n > MAX_BLOB) {
            throw new IOException("blob too large: " + n);
        }
        byte[] b = new byte[n];
        in.readFully(b);
        return b;
    }

    // ---- commands and entries ---------------------------------------------------------------

    public static void writeCommand(DataOutputStream out, Command c) throws IOException {
        out.writeByte(c.type().ordinal());
        writeString(out, c.clientId());
        out.writeLong(c.seq());
        writeString(out, c.key());
        writeString(out, c.value());
        writeString(out, c.expected());
    }

    public static Command readCommand(DataInputStream in) throws IOException {
        int t = in.readUnsignedByte();
        Command.Type[] types = Command.Type.values();
        if (t >= types.length) {
            throw new IOException("bad command type " + t);
        }
        String clientId = readString(in);
        long seq = in.readLong();
        String key = readString(in);
        String value = readString(in);
        String expected = readString(in);
        return new Command(types[t], clientId, seq, key, value, expected);
    }

    public static void writeEntry(DataOutputStream out, LogEntry e) throws IOException {
        out.writeLong(e.term());
        writeCommand(out, e.command());
    }

    public static LogEntry readEntry(DataInputStream in) throws IOException {
        long term = in.readLong();
        return new LogEntry(term, readCommand(in));
    }

    public static void writeEntries(DataOutputStream out, List<LogEntry> entries) throws IOException {
        out.writeInt(entries.size());
        for (LogEntry e : entries) {
            writeEntry(out, e);
        }
    }

    public static List<LogEntry> readEntries(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > 10_000_000) {
            throw new IOException("bad entry count " + n);
        }
        List<LogEntry> list = new ArrayList<>(Math.min(n, 1024));
        for (int i = 0; i < n; i++) {
            list.add(readEntry(in));
        }
        return list;
    }

    // ---- messages ---------------------------------------------------------------------------

    public static void writeMessage(DataOutputStream out, Message m) throws IOException {
        switch (m) {
            case RequestVote v -> {
                out.writeByte(1);
                out.writeLong(v.term());
                writeString(out, v.candidateId());
                out.writeLong(v.lastLogIndex());
                out.writeLong(v.lastLogTerm());
            }
            case RequestVoteReply r -> {
                out.writeByte(2);
                out.writeLong(r.term());
                out.writeBoolean(r.granted());
            }
            case AppendEntries a -> {
                out.writeByte(3);
                out.writeLong(a.term());
                writeString(out, a.leaderId());
                out.writeLong(a.prevLogIndex());
                out.writeLong(a.prevLogTerm());
                writeEntries(out, a.entries());
                out.writeLong(a.leaderCommit());
            }
            case AppendEntriesReply r -> {
                out.writeByte(4);
                out.writeLong(r.term());
                out.writeBoolean(r.success());
                out.writeLong(r.matchIndex());
                out.writeLong(r.conflictIndex());
                out.writeLong(r.conflictTerm());
            }
            case InstallSnapshot s -> {
                out.writeByte(5);
                out.writeLong(s.term());
                writeString(out, s.leaderId());
                out.writeLong(s.lastIncludedIndex());
                out.writeLong(s.lastIncludedTerm());
                writeBytes(out, s.data());
            }
            case InstallSnapshotReply r -> {
                out.writeByte(6);
                out.writeLong(r.term());
                out.writeLong(r.lastIncludedIndex());
            }
        }
    }

    public static Message readMessage(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        return switch (tag) {
            case 1 -> new RequestVote(in.readLong(), readString(in), in.readLong(), in.readLong());
            case 2 -> new RequestVoteReply(in.readLong(), in.readBoolean());
            case 3 -> {
                long term = in.readLong();
                String leader = readString(in);
                long prevIdx = in.readLong();
                long prevTerm = in.readLong();
                List<LogEntry> entries = readEntries(in);
                yield new AppendEntries(term, leader, prevIdx, prevTerm, entries, in.readLong());
            }
            case 4 -> new AppendEntriesReply(in.readLong(), in.readBoolean(), in.readLong(), in.readLong(), in.readLong());
            case 5 -> new InstallSnapshot(in.readLong(), readString(in), in.readLong(), in.readLong(), readBytes(in));
            case 6 -> new InstallSnapshotReply(in.readLong(), in.readLong());
            default -> throw new IOException("unknown message tag " + tag);
        };
    }

    public static byte[] encode(Message m) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            writeMessage(new DataOutputStream(bos), m);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Message decode(byte[] bytes) throws IOException {
        return readMessage(new DataInputStream(new ByteArrayInputStream(bytes)));
    }
}
