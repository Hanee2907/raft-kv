package raftkv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Crash-safe file storage: write the full state to a temp file, fsync it, atomically rename it over
 * the live file, then fsync the directory. A CRC detects torn or corrupted files on load.
 *
 * <p>Simplicity over speed: every save rewrites the whole state. A production system would use an
 * append-only write-ahead log with periodic compaction.
 */
public final class FileStorage implements Storage {
    private static final int MAGIC = 0x52414654; // "RAFT"
    private final Path dir;
    private final Path file;
    private final Path tmp;

    public FileStorage(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
        this.file = dir.resolve("raft-state.bin");
        this.tmp = dir.resolve("raft-state.tmp");
    }

    @Override
    public PersistentState load() throws IOException {
        if (!Files.exists(file)) {
            return PersistentState.empty();
        }
        byte[] all = Files.readAllBytes(file);
        if (all.length < 12) {
            throw new IOException("state file too short");
        }
        int bodyLen = all.length - 8;
        CRC32 crc = new CRC32();
        crc.update(all, 0, bodyLen);
        long stored = new DataInputStream(new ByteArrayInputStream(all, bodyLen, 8)).readLong();
        if (stored != crc.getValue()) {
            throw new IOException("state file checksum mismatch (corrupt)");
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(all, 0, bodyLen));
        if (in.readInt() != MAGIC) {
            throw new IOException("bad magic");
        }
        long term = in.readLong();
        String votedFor = Codec.readString(in);
        long snapIdx = in.readLong();
        long snapTerm = in.readLong();
        byte[] snap = Codec.readBytes(in);
        List<LogEntry> log = Codec.readEntries(in);
        return new PersistentState(term, votedFor, snapIdx, snapTerm, snap, log);
    }

    @Override
    public void save(PersistentState s) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.writeInt(MAGIC);
        out.writeLong(s.term());
        Codec.writeString(out, s.votedFor());
        out.writeLong(s.snapshotIndex());
        out.writeLong(s.snapshotTerm());
        Codec.writeBytes(out, s.snapshot());
        Codec.writeEntries(out, s.log());
        out.flush();
        byte[] body = bos.toByteArray();
        CRC32 crc = new CRC32();
        crc.update(body);

        try (FileOutputStream fos = new FileOutputStream(tmp.toFile())) {
            fos.write(body);
            new DataOutputStream(fos).writeLong(crc.getValue());
            fos.getFD().sync();
        }
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true); // make the rename durable; not supported on every platform
        } catch (IOException | UnsupportedOperationException ignored) {
            // best effort
        }
    }
}
