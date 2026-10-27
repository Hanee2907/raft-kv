package raftkv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Three real servers on localhost: real TCP between them, real files for durability, and the real
 * client protocol. Kills the leader, checks failover, restarts it from its data directory.
 */
final class TcpClusterTest {
    private TcpClusterTest() {
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    static void run() throws Exception {
        Path root = Files.createTempDirectory("raftkv-tcp");
        List<KvServer.Peer> peers = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            peers.add(new KvServer.Peer("n" + i, "127.0.0.1", freePort(), freePort()));
        }
        List<InetSocketAddress> clientAddrs = new ArrayList<>();
        for (KvServer.Peer p : peers) {
            clientAddrs.add(new InetSocketAddress(p.host(), p.clientPort()));
        }
        RaftNode.Config cfg = new RaftNode.Config();
        cfg.electionTimeoutMinMs = 150;
        cfg.electionTimeoutMaxMs = 300;
        cfg.heartbeatMs = 40;
        cfg.snapshotThreshold = 30; // force snapshots + file compaction during the test

        Map<String, KvServer> servers = new TreeMap<>();
        try {
            for (KvServer.Peer p : peers) {
                servers.put(p.id(), new KvServer(p.id(), peers, root.resolve(p.id()), cfg.copy()));
            }

            Check.eq("OK", KvClient.call(clientAddrs, "PUT greeting hello world", 10_000), "put");
            Check.eq("OK hello world", KvClient.call(clientAddrs, "GET greeting", 10_000), "get (value with spaces)");
            Check.eq("NIL", KvClient.call(clientAddrs, "GET missing", 10_000), "get of absent key");
            Check.eq("OK", KvClient.call(clientAddrs, "CAS counter NIL 1", 10_000), "cas on absent key");
            Check.eq("FAIL 1", KvClient.call(clientAddrs, "CAS counter 7 2", 10_000), "cas with wrong expected");
            Check.eq("OK", KvClient.call(clientAddrs, "CAS counter 1 2", 10_000), "cas with right expected");

            for (int i = 0; i < 100; i++) {
                Check.eq("OK", KvClient.call(clientAddrs, "PUT key" + i + " value" + i, 10_000), "bulk put " + i);
            }

            // find and kill the leader
            String leaderId = null;
            for (Map.Entry<String, KvServer> e : servers.entrySet()) {
                if (e.getValue().node().status().role() == RaftNode.Role.LEADER) {
                    leaderId = e.getKey();
                }
            }
            Check.that(leaderId != null, "a leader exists");
            servers.remove(leaderId).close();

            Check.eq("OK value50", KvClient.call(clientAddrs, "GET key50", 15_000), "data readable after leader died");
            Check.eq("OK", KvClient.call(clientAddrs, "PUT afterFailover yes", 15_000), "writes work with 2 of 3 nodes");

            // bring the dead node back from its data directory (snapshot + log on disk)
            for (KvServer.Peer p : peers) {
                if (p.id().equals(leaderId)) {
                    servers.put(p.id(), new KvServer(p.id(), peers, root.resolve(p.id()), cfg.copy()));
                }
            }
            final String revived = leaderId;
            Check.eventually("revived node catches up through its own disk state + leader", 10_000, () -> {
                RaftNode.Status s = servers.get(revived).node().status();
                long maxApplied = 0;
                for (KvServer sv : servers.values()) {
                    maxApplied = Math.max(maxApplied, sv.node().status().lastApplied());
                }
                return s.lastApplied() == maxApplied && s.lastApplied() > 100;
            });
            Map<String, String> a = servers.get(revived).node().stateView().get();
            for (KvServer sv : servers.values()) {
                Check.eq(a, sv.node().stateView().get(), "all replicas hold identical state");
            }
            Check.eq("hello world", a.get("greeting"), "restored value");
            Check.eq("2", a.get("counter"), "restored cas result");
            Check.eq("yes", a.get("afterFailover"), "write made while the node was dead");
        } finally {
            for (KvServer s : servers.values()) {
                s.close();
            }
            AllTests.deleteRecursively(root);
        }
    }
}
