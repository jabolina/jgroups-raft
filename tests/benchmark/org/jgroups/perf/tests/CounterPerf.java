package org.jgroups.perf.tests;

import org.jgroups.annotations.Property;
import org.jgroups.perf.CommandLineOptions;
import org.jgroups.perf.harness.AbstractRaftBenchmark;
import org.jgroups.perf.harness.hyperfoil.internal.RaftBenchmarkStepBuilder;
import org.jgroups.raft.blocks.CounterService;
import org.jgroups.raft.blocks.RaftAsyncCounter;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;

import io.hyperfoil.api.config.StepBuilder;

public class CounterPerf extends AbstractRaftBenchmark {

    private static final String COUNTER = "counter";
    private static final Field RANGE;

    static {
        try {
            RANGE = Util.getField(CounterPerf.class, "range", true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final CounterService counterService;
    @Property
    protected int range = 10;
    private RaftAsyncCounter counter;

    public CounterPerf(CommandLineOptions cmd) throws Throwable {
        super(cmd);
        counterService = new CounterService(channel);
    }

    public static boolean tossWeightedCoin(double probability) {
        if (probability >= 1)
            return true;
        if (probability <= 0)
            return false;
        long r = Util.random(1000);
        long cutoff = (long) (probability * 1000);
        return r <= cutoff;
    }

    @Override
    public RaftBenchmarkStepBuilder createBenchmarkStep() {
        try {
            counter = counterService.getOrCreateCounter(COUNTER, 0).async();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new RaftBenchmarkStepBuilder().withMethod(this::benchmarkOperation);
    }

    @Override
    public String extendedEventLoopHeader() {
        if (counter == null)
            return String.format("[r] Range (%d)", range);

        return String.format("[r] Range (%d) (counter=%s)", range, counter);
    }

    @Override
    public void extendedEventLoop(int c) throws Throwable {
        if (c != 'r') {
            System.out.printf("Unknown option: %c%n", c);
            return;
        }

        changeFieldAcrossCluster(RANGE, Util.readIntFromStdin("range: "));
    }

    private CompletableFuture<?> benchmarkOperation() {
        long v = getDelta();
        return counter.addAndGet(v).toCompletableFuture();
    }

    private int getDelta() {
        long random = Util.random(range);
        int retval = (int) (tossWeightedCoin(.5) ? -random : random);
        if (retval < 0 && counter.getLocal() < 0)
            retval = -retval;
        return retval;
    }
}
