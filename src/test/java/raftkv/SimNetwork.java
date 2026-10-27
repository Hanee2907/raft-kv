package raftkv;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import raftkv.Messages.Message;

/**
 * In-process network for tests. Delivers messages after a random delay, on a small thread pool (so
 * messages are reordered), and can drop, duplicate, delay-a-lot, and partition links on demand.
 * Messages already in flight when a partition starts are still delivered, like packets already
 * on the wire.
 */
public final class SimNetwork implements AutoCloseable {
    private final ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(4, r -> {
        Thread t = new Thread(r, "simnet");
        t.setDaemon(true);
        return t;
    });
    private final ConcurrentHashMap<String, BiConsumer<String, Message>> receivers = new ConcurrentHashMap<>();
    private final Set<String> blocked = ConcurrentHashMap.newKeySet();

    public volatile double dropRate = 0;
    public volatile double dupRate = 0;
    public volatile double slowRate = 0;
    public volatile int minDelayMs = 1;
    public volatile int maxDelayMs = 6;
    public volatile int slowDelayMs = 250;

    public final AtomicLong sent = new AtomicLong();
    public final AtomicLong dropped = new AtomicLong();

    public Transport transportFor(String self) {
        return new Transport() {
            @Override
            public void send(String to, Message message) {
                route(self, to, message);
            }

            @Override
            public void setReceiver(BiConsumer<String, Message> receiver) {
                if (receiver == null) {
                    receivers.remove(self);
                } else {
                    receivers.put(self, receiver);
                }
            }
        };
    }

    // ---- fault injection --------------------------------------------------------------------

    public void block(String a, String b) {
        blocked.add(a + ">" + b);
        blocked.add(b + ">" + a);
    }

    /** Cuts every link between the two groups (links inside each group stay up). */
    public void partition(Collection<String> groupA, Collection<String> groupB) {
        for (String a : groupA) {
            for (String b : groupB) {
                block(a, b);
            }
        }
    }

    public void isolate(String node, Collection<String> all) {
        for (String other : all) {
            if (!other.equals(node)) {
                block(node, other);
            }
        }
    }

    public void heal() {
        blocked.clear();
    }

    public boolean isConnected(String a, String b) {
        return !blocked.contains(a + ">" + b);
    }

    // ---- delivery ---------------------------------------------------------------------------

    private void route(String from, String to, Message msg) {
        sent.incrementAndGet();
        if (blocked.contains(from + ">" + to)) {
            dropped.incrementAndGet();
            return;
        }
        ThreadLocalRandom r = ThreadLocalRandom.current();
        if (r.nextDouble() < dropRate) {
            dropped.incrementAndGet();
            return;
        }
        int copies = r.nextDouble() < dupRate ? 2 : 1;
        for (int i = 0; i < copies; i++) {
            int delay = r.nextDouble() < slowRate
                    ? r.nextInt(Math.max(1, slowDelayMs))
                    : minDelayMs + r.nextInt(Math.max(1, maxDelayMs - minDelayMs + 1));
            try {
                pool.schedule(() -> {
                    BiConsumer<String, Message> rcv = receivers.get(to);
                    if (rcv != null) {
                        rcv.accept(from, msg);
                    }
                }, delay, TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.RejectedExecutionException ignored) {
                // network shut down
            }
        }
    }

    @Override
    public void close() {
        pool.shutdownNow();
    }
}
