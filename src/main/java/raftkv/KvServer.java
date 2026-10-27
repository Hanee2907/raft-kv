package raftkv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A runnable Raft key-value server: a {@link RaftNode} on {@link TcpTransport} + {@link FileStorage},
 * plus a line-based client protocol.
 *
 * <pre>
 *   GET key                  -> OK value | NIL
 *   PUT key value...         -> OK
 *   CAS key expected new...  -> OK | FAIL current|NIL      (expected = NIL means "key absent")
 *   STATUS                   -> OK id role term leader commit applied
 *   any write/read to a non-leader -> NOT_LEADER host:clientPort | NOT_LEADER NONE
 *   outcome unknown (leader lost mid-flight) -> TIMEOUT
 * </pre>
 *
 * Every GET goes through the Raft log, so reads are linearizable.
 */
public final class KvServer implements AutoCloseable {

    public record Peer(String id, String host, int raftPort, int clientPort) {
        public static Peer parse(String spec) {
            String[] kv = spec.split("=", 2);
            String[] hp = kv[1].split(":");
            if (kv.length != 2 || hp.length != 3) {
                throw new IllegalArgumentException("expected id=host:raftPort:clientPort but got " + spec);
            }
            return new Peer(kv[0], hp[0], Integer.parseInt(hp[1]), Integer.parseInt(hp[2]));
        }
    }

    private static final String NIL = "NIL";

    private final String id;
    private final Map<String, Peer> cluster = new LinkedHashMap<>();
    private final TcpTransport transport;
    private final RaftNode node;
    private final ServerSocket clientSocket;
    private volatile boolean closed;

    public KvServer(String id, List<Peer> peers, Path dataDir, RaftNode.Config cfg) throws IOException {
        this.id = id;
        Map<String, InetSocketAddress> raftAddrs = new LinkedHashMap<>();
        for (Peer p : peers) {
            cluster.put(p.id(), p);
            raftAddrs.put(p.id(), new InetSocketAddress(p.host(), p.raftPort()));
        }
        Peer self = cluster.get(id);
        if (self == null) {
            throw new IllegalArgumentException("node id " + id + " is not in the cluster list");
        }
        this.transport = new TcpTransport(id, raftAddrs);
        this.node = new RaftNode(id, new ArrayList<>(cluster.keySet()), transport, new FileStorage(dataDir), cfg);
        this.clientSocket = new ServerSocket();
        clientSocket.setReuseAddress(true);
        clientSocket.bind(new InetSocketAddress(self.host(), self.clientPort()));
        node.start();
        Thread.startVirtualThread(this::acceptLoop);
    }

    public RaftNode node() {
        return node;
    }

    @Override
    public void close() {
        closed = true;
        try {
            clientSocket.close();
        } catch (IOException ignored) {
            // closing
        }
        node.close();
        transport.close();
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket s = clientSocket.accept();
                Thread.startVirtualThread(() -> serve(s));
            } catch (IOException e) {
                if (closed) {
                    return;
                }
            }
        }
    }

    private void serve(Socket s) {
        try (s;
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {
            String line;
            while ((line = in.readLine()) != null) {
                out.println(handle(line.strip()));
            }
        } catch (IOException ignored) {
            // client went away
        }
    }

    private String handle(String line) {
        String[] t = line.split(" ", 2);
        String op = t[0].toUpperCase();
        String rest = t.length > 1 ? t[1] : "";
        try {
            switch (op) {
                case "STATUS": {
                    RaftNode.Status st = node.status();
                    return "OK " + st.id() + " " + st.role() + " " + st.term() + " "
                            + (st.leaderId() == null ? "NONE" : st.leaderId()) + " " + st.commitIndex() + " " + st.lastApplied();
                }
                case "GET": {
                    if (rest.isEmpty()) {
                        return "ERR usage: GET key";
                    }
                    return render(node.submit(Command.get("", 0, rest)), "GET");
                }
                case "PUT": {
                    String[] kv = rest.split(" ", 2);
                    if (kv.length < 2) {
                        return "ERR usage: PUT key value";
                    }
                    return render(node.submit(Command.put("", 0, kv[0], kv[1])), "PUT");
                }
                case "CAS": {
                    String[] p = rest.split(" ", 3);
                    if (p.length < 3) {
                        return "ERR usage: CAS key expected new   (use NIL for expected-absent)";
                    }
                    String expected = NIL.equals(p[1]) ? null : p[1];
                    return render(node.submit(Command.cas("", 0, p[0], expected, p[2])), "CAS");
                }
                default:
                    return "ERR unknown command (GET, PUT, CAS, STATUS)";
            }
        } catch (RuntimeException e) {
            return "ERR " + e;
        }
    }

    private String render(java.util.concurrent.CompletableFuture<Result> f, String op) {
        Result r;
        try {
            r = f.get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return "TIMEOUT";
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "ERR interrupted";
        } catch (ExecutionException e) {
            return "ERR " + e.getCause();
        }
        switch (r.status()) {
            case NOT_LEADER: {
                Peer leader = r.leaderHint() == null ? null : cluster.get(r.leaderHint());
                return "NOT_LEADER " + (leader == null ? "NONE" : leader.host() + ":" + leader.clientPort());
            }
            case INDETERMINATE:
                return "TIMEOUT";
            default:
                return switch (op) {
                    case "GET" -> r.value() == null ? NIL : "OK " + r.value();
                    case "CAS" -> r.success() ? "OK" : "FAIL " + (r.value() == null ? NIL : r.value());
                    default -> "OK";
                };
        }
    }

    // ---- main -------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        String id = null;
        String dataDir = null;
        List<Peer> peers = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--id" -> id = args[++i];
                case "--data-dir" -> dataDir = args[++i];
                case "--cluster" -> {
                    for (String spec : args[++i].split(",")) {
                        peers.add(Peer.parse(spec.trim()));
                    }
                }
                default -> throw new IllegalArgumentException("unknown argument " + args[i]);
            }
        }
        if (id == null || dataDir == null || peers.isEmpty()) {
            System.err.println("usage: KvServer --id n1 --data-dir ./data/n1 "
                    + "--cluster n1=127.0.0.1:7001:8001,n2=127.0.0.1:7002:8002,n3=127.0.0.1:7003:8003");
            System.exit(2);
        }
        KvServer server = new KvServer(id, peers, Path.of(dataDir), new RaftNode.Config());
        System.out.println("node " + id + " up; clients on " + server.cluster.get(id).clientPort());
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }
}
