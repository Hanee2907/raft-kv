package raftkv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal client for {@link KvServer}. Finds the leader by following NOT_LEADER redirects and by
 * rotating through the server list on connection errors.
 *
 * <p>Retry policy: NOT_LEADER and connection failures are always safe to retry (nothing was
 * applied). TIMEOUT means "outcome unknown", so it is retried only for GET and PUT, which are safe to
 * repeat, and returned to the caller for CAS.
 */
public final class KvClient {
    private KvClient() {
    }

    public static String call(List<InetSocketAddress> servers, String line, long timeoutMs) {
        List<InetSocketAddress> known = new ArrayList<>(servers);
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        boolean isCas = line.regionMatches(true, 0, "CAS", 0, 3);
        int idx = 0;
        String last = "ERR no response";
        while (System.nanoTime() < deadline) {
            InetSocketAddress target = known.get(idx % known.size());
            String resp = once(target, line);
            if (resp == null) {
                idx++;            // could not talk to it: try the next server
                last = "ERR connection failed";
            } else if (resp.startsWith("NOT_LEADER")) {
                String hint = resp.substring("NOT_LEADER".length()).strip();
                if (!hint.equals("NONE")) {
                    int colon = hint.lastIndexOf(':');
                    InetSocketAddress leader = new InetSocketAddress(hint.substring(0, colon),
                            Integer.parseInt(hint.substring(colon + 1)));
                    int at = known.indexOf(leader);
                    if (at < 0) {
                        known.add(leader);
                        at = known.size() - 1;
                    }
                    idx = at;
                } else {
                    idx++;
                }
                last = resp;
            } else if (resp.equals("TIMEOUT") && !isCas) {
                idx++;
                last = resp;
            } else {
                return resp;
            }
            try {
                Thread.sleep(40);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return "ERR interrupted";
            }
        }
        return last;
    }

    private static String once(InetSocketAddress addr, String line) {
        try (Socket s = new Socket()) {
            s.connect(addr, 500);
            s.setSoTimeout(5000);
            PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            out.println(line);
            return in.readLine();
        } catch (IOException e) {
            return null;
        }
    }

    public static List<InetSocketAddress> parseServers(String csv) {
        List<InetSocketAddress> list = new ArrayList<>();
        for (String hp : csv.split(",")) {
            String[] p = hp.trim().split(":");
            list.add(new InetSocketAddress(p[0], Integer.parseInt(p[1])));
        }
        return list;
    }

    /** {@code KvClient --servers host:port,host:port GET key} (or no command for an interactive prompt). */
    public static void main(String[] args) throws IOException {
        if (args.length < 2 || !args[0].equals("--servers")) {
            System.err.println("usage: KvClient --servers 127.0.0.1:8001,127.0.0.1:8002 [GET key | PUT key value | CAS key expected new]");
            System.exit(2);
        }
        List<InetSocketAddress> servers = parseServers(args[1]);
        if (args.length > 2) {
            System.out.println(call(servers, String.join(" ", java.util.Arrays.copyOfRange(args, 2, args.length)), 10_000));
            return;
        }
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("> ");
        String line;
        while ((line = stdin.readLine()) != null) {
            if (!line.isBlank()) {
                System.out.println(call(servers, line.strip(), 10_000));
            }
            System.out.print("> ");
        }
    }
}
