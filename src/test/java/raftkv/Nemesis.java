package raftkv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The fault injector. On a random schedule it cuts the network in assorted nasty ways, kills nodes,
 * restarts them, and heals, until stopped. Every action is logged so a failing run can be read.
 */
public final class Nemesis implements Runnable {
    private final SimCluster cluster;
    private final Random rnd;
    private final long startNs = System.nanoTime();
    private volatile boolean stop;
    private final List<String> actions = new CopyOnWriteArrayList<>();

    public Nemesis(SimCluster cluster, long seed) {
        this.cluster = cluster;
        this.rnd = new Random(seed);
    }

    public List<String> actions() {
        return actions;
    }

    public void stop() {
        stop = true;
    }

    private void log(String s) {
        actions.add(String.format("[+%5.2fs] %s", (System.nanoTime() - startNs) / 1e9, s));
    }

    @Override
    public void run() {
        try {
            while (!stop) {
                Thread.sleep(250 + rnd.nextInt(700));
                if (stop) {
                    break;
                }
                switch (rnd.nextInt(8)) {
                    case 0 -> splitHalves();
                    case 1 -> isolateLeader();
                    case 2 -> isolateRandom();
                    case 3 -> bridge();
                    case 4, 5 -> crashRandom();
                    case 6 -> restartSome();
                    default -> heal();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Called after stop(): put the world back together. */
    public void healEverything() {
        cluster.net().heal();
        cluster.restartAll();
        log("healed network, restarted all nodes");
    }

    private void heal() {
        cluster.net().heal();
        log("heal network");
    }

    private void splitHalves() {
        cluster.net().heal();
        List<String> ids = new ArrayList<>(cluster.ids());
        Collections.shuffle(ids, rnd);
        int minority = 1 + rnd.nextInt(Math.max(1, ids.size() / 2));
        List<String> a = ids.subList(0, minority);
        List<String> b = ids.subList(minority, ids.size());
        cluster.net().partition(a, b);
        log("partition " + a + " | " + b);
    }

    private void isolateLeader() {
        cluster.net().heal();
        String leader = cluster.leader();
        if (leader == null) {
            return;
        }
        cluster.net().isolate(leader, cluster.ids());
        log("isolate leader " + leader);
    }

    private void isolateRandom() {
        cluster.net().heal();
        String victim = cluster.ids().get(rnd.nextInt(cluster.ids().size()));
        cluster.net().isolate(victim, cluster.ids());
        log("isolate " + victim);
    }

    /** Two groups that cannot see each other, joined only through one bridge node that can see both. */
    private void bridge() {
        cluster.net().heal();
        List<String> ids = new ArrayList<>(cluster.ids());
        Collections.shuffle(ids, rnd);
        String bridge = ids.remove(0);
        int half = ids.size() / 2;
        List<String> a = ids.subList(0, half);
        List<String> b = ids.subList(half, ids.size());
        cluster.net().partition(a, b);
        log("bridge via " + bridge + ": " + a + " | " + b);
    }

    private int downCount() {
        int d = 0;
        for (String id : cluster.ids()) {
            if (!cluster.isUp(id)) {
                d++;
            }
        }
        return d;
    }

    private void crashRandom() {
        int maxDown = (cluster.ids().size() - 1) / 2; // always leave a majority alive
        if (downCount() >= maxDown) {
            return;
        }
        List<String> up = new ArrayList<>();
        for (String id : cluster.ids()) {
            if (cluster.isUp(id)) {
                up.add(id);
            }
        }
        String victim = rnd.nextDouble() < 0.5 && cluster.leader() != null && up.contains(cluster.leader())
                ? cluster.leader() : up.get(rnd.nextInt(up.size()));
        cluster.crash(victim);
        log("crash " + victim);
    }

    private void restartSome() {
        for (String id : cluster.ids()) {
            if (!cluster.isUp(id)) {
                cluster.restart(id);
                log("restart " + id);
                return;
            }
        }
    }
}
