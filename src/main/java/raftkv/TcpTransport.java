package raftkv;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import raftkv.Messages.Message;

/**
 * Raft over plain TCP using only the JDK (blocking sockets on virtual threads).
 *
 * <p>Wire format: {@code [int length][sender id string][message]} using {@link Codec}. Each peer gets a
 * bounded outbound queue and a dedicated sender that connects lazily and reconnects with backoff;
 * if a peer is slow or down, messages are dropped (Raft retries by itself). There is no
 * authentication or encryption: run it inside a trusted network or add TLS.
 */
public final class TcpTransport implements Transport, AutoCloseable {
    private static final int MAX_FRAME = 128 * 1024 * 1024;

    private final String self;
    private final Map<String, InetSocketAddress> addrs;
    private final ServerSocket server;
    private final Map<String, Sender> senders = new ConcurrentHashMap<>();
    private final Map<Socket, Boolean> inbound = new ConcurrentHashMap<>();
    private volatile BiConsumer<String, Message> receiver;
    private volatile boolean closed;

    public TcpTransport(String self, Map<String, InetSocketAddress> addrs) throws IOException {
        this.self = self;
        this.addrs = addrs;
        this.server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(addrs.get(self));
        Thread.startVirtualThread(this::acceptLoop);
    }

    @Override
    public void setReceiver(BiConsumer<String, Message> receiver) {
        this.receiver = receiver;
    }

    @Override
    public void send(String to, Message message) {
        if (closed || !addrs.containsKey(to)) {
            return;
        }
        byte[] frame;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);
            Codec.writeString(out, self);
            Codec.writeMessage(out, message);
            out.flush();
            frame = bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        senders.computeIfAbsent(to, Sender::new).queue.offer(frame); // full queue => drop
    }

    @Override
    public void close() {
        closed = true;
        try {
            server.close();
        } catch (IOException ignored) {
            // closing
        }
        for (Socket s : inbound.keySet()) {
            closeQuietly(s);
        }
        for (Sender s : senders.values()) {
            s.shutdown();
        }
    }

    // ---- inbound ----------------------------------------------------------------------------

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket s = server.accept();
                s.setTcpNoDelay(true);
                inbound.put(s, Boolean.TRUE);
                Thread.startVirtualThread(() -> readLoop(s));
            } catch (IOException e) {
                if (closed) {
                    return;
                }
            }
        }
    }

    private void readLoop(Socket s) {
        try (s) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
            while (!closed) {
                int len = in.readInt();
                if (len <= 0 || len > MAX_FRAME) {
                    return; // protocol violation: drop the connection
                }
                byte[] frame = new byte[len];
                in.readFully(frame);
                DataInputStream fi = new DataInputStream(new ByteArrayInputStream(frame));
                String from = Codec.readString(fi);
                Message m = Codec.readMessage(fi);
                BiConsumer<String, Message> r = receiver;
                if (r != null && from != null) {
                    r.accept(from, m);
                }
            }
        } catch (IOException ignored) {
            // peer went away or sent garbage
        } finally {
            inbound.remove(s);
        }
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
            // closing
        }
    }

    // ---- outbound ---------------------------------------------------------------------------

    private final class Sender {
        final String peer;
        final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(2048);
        private volatile Socket socket;
        private final Thread thread;

        Sender(String peer) {
            this.peer = peer;
            this.thread = Thread.startVirtualThread(this::loop);
        }

        void shutdown() {
            thread.interrupt();
            Socket s = socket;
            if (s != null) {
                closeQuietly(s);
            }
        }

        private void loop() {
            DataOutputStream out = null;
            try {
                while (!closed) {
                    byte[] frame = queue.take();
                    if (out == null) {
                        try {
                            Socket s = new Socket();
                            s.setTcpNoDelay(true);
                            s.connect(addrs.get(peer), 300);
                            socket = s;
                            out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                        } catch (IOException e) {
                            queue.clear();    // peer is down: don't build up a stale backlog
                            Thread.sleep(100);
                            continue;
                        }
                    }
                    try {
                        out.writeInt(frame.length);
                        out.write(frame);
                        out.flush();
                    } catch (IOException e) {
                        closeQuietly(socket);
                        out = null;
                    }
                }
            } catch (InterruptedException ignored) {
                // shutting down
            }
        }
    }
}
