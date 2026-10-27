package raftkv;

/**
 * Runs the Jepsen-style experiment over many seeds, to hunt for rare interleavings.
 * Usage: {@code java -cp out/main:out/test raftkv.Soak <firstSeed> <count> [durationMs]}
 * Exits non-zero on the first failing seed and prints everything needed to understand it.
 */
public final class Soak {
    private Soak() {
    }

    public static void main(String[] args) throws Exception {
        long first = args.length > 0 ? Long.parseLong(args[0]) : 1;
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        long duration = args.length > 2 ? Long.parseLong(args[2]) : 8_000;
        int failures = 0;
        for (long seed = first; seed < first + count; seed++) {
            JepsenRunner.Config cfg = new JepsenRunner.Config();
            cfg.seed = seed;
            cfg.durationMs = duration;
            JepsenRunner.Outcome o = JepsenRunner.run(cfg);
            boolean ok = o.safetyViolations.isEmpty() && o.converged
                    && o.report.verdict() == LinearizabilityChecker.Verdict.LINEARIZABLE;
            System.out.printf("seed %-4d %s  %s%n", seed, ok ? "ok  " : "FAIL", o.summary());
            if (!ok) {
                failures++;
                if (!o.safetyViolations.isEmpty()) {
                    System.out.println("  safety violations: " + o.safetyViolations);
                }
                if (!o.converged) {
                    System.out.println("  did not converge: " + o.convergenceDetail);
                }
                System.out.println("  " + o.report.toString().replace("\n", "\n  "));
                System.out.println("  nemesis log:");
                o.nemesisLog.forEach(l -> System.out.println("    " + l));
            }
        }
        System.out.println(failures == 0 ? "all " + count + " seeds passed" : failures + " of " + count + " seeds FAILED");
        System.exit(failures == 0 ? 0 : 1);
    }
}
