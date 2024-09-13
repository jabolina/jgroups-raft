package org.jgroups.perf.harness;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.Version;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.perf.CommandLineOptions;
import org.jgroups.perf.harness.hyperfoil.config.RaftPluginBuilder;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.tests.DummyStateMachine;
import org.jgroups.tests.perf.PerfUtil;
import org.jgroups.util.Bits;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Streamable;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.hyperfoil.api.config.Benchmark;
import io.hyperfoil.api.config.BenchmarkBuilder;
import io.hyperfoil.api.config.PhaseBuilder;
import io.hyperfoil.api.config.StepBuilder;
import io.hyperfoil.api.statistics.StatisticsSnapshot;
import io.hyperfoil.core.impl.LocalSimulationRunner;
import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.Histogram;

/**
 * Default harness for a benchmark.
 * <p>
 * Bootstrap the benchmark with the default configuration and methods for creating the cluster. Provides the functionalities
 * for updating configuration, retrieving the current configuration from the coordinator, and starting the benchmark in the cluster.
 * </p>
 * <p>
 * Classes extending the harness have a few methods available to extend the configuration:
 * <ul>
 *     <li>{@link #syncBenchmark(ThreadFactory)} and {@link #asyncBenchmark(ThreadFactory)}: Create the synchronous and
 *          asynchronous instances for running the benchmark, respectively;</li>
 *     <li>{@link #extendedEventLoop(int)}: Extend the event loop to parse additional options;</li>
 *     <li>{@link #extendedEventLoopHeader()}: Extend the event loop options message;</li>
 *     <li>{@link #clear()}: Clear resources created by the benchmark at exit.</li>
 * </ul>
 * </p>
 *
 * @author José Bolina
 */
public abstract class AbstractRaftBenchmark implements Receiver {

    protected static final Field NUM_THREADS, TIME, TIMEOUT, PRINT_INVOKERS, PRINT_DETAILS, BENCHMARK;
    private static final Method[] METHODS = new Method[4];
    private static final short START = 0;
    private static final short GET_CONFIG = 1;
    private static final short SET = 2;
    private static final short QUIT_ALL = 3;
    private static final String CLUSTER_NAME;
    private static final String BASE_EVENT_LOOP =
            "[1] Start test [2] View [4] Threads (%d) [6] Time (%s)" +
                    "\n[t] incr timeout (%s) [d] details (%b)  [i] print updaters (%b)" +
                    "\n[b] benchmark mode (%s) [v] Version" +
                    "\n%s" +
                    "\n[x] Exit [X] Exit all";

    static {
        try {
            METHODS[START] = AbstractRaftBenchmark.class.getMethod("startTest");
            METHODS[GET_CONFIG] = AbstractRaftBenchmark.class.getMethod("getConfig");
            METHODS[SET] = AbstractRaftBenchmark.class.getMethod("set", String.class, Object.class);
            METHODS[QUIT_ALL] = AbstractRaftBenchmark.class.getMethod("quitAll");

            NUM_THREADS = Util.getField(AbstractRaftBenchmark.class, "num_threads", true);
            TIME = Util.getField(AbstractRaftBenchmark.class, "time", true);
            TIMEOUT = Util.getField(AbstractRaftBenchmark.class, "timeout", true);
            PRINT_INVOKERS = Util.getField(AbstractRaftBenchmark.class, "print_updaters", true);
            PRINT_DETAILS = Util.getField(AbstractRaftBenchmark.class, "print_details", true);
            BENCHMARK = Util.getField(AbstractRaftBenchmark.class, "benchmark", true);

            PerfUtil.init();
            ClassConfigurator.addIfAbsent((short) 1050, UpdateResult.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        CLUSTER_NAME = MethodHandles.lookup().lookupClass().getSimpleName();
    }

    protected final JChannel channel;
    private final String histogramPath;
    private final ThreadFactory threadFactory;
    private final RpcDispatcher disp;

    @Property
    protected int num_threads = 100;

    @Property
    protected int time = 30; // in seconds

    @Property
    protected boolean print_updaters;

    @Property
    protected boolean print_details;

    @Property
    protected long timeout = 60_000; // ms

    @Property
    protected String benchmark = "sync";

    private volatile boolean looping = true;

    public AbstractRaftBenchmark(CommandLineOptions cmd) throws Throwable {
        this.histogramPath = cmd.getHistogramPath();
        if (histogramPath != null)
            System.out.printf("Histogram enabled! Storing in '%s'%n", histogramPath);

        this.channel = new JChannel(cmd.getProps()).name(cmd.getName());

        TP transport = channel.getProtocolStack().getTransport();
        boolean useVirtualThreads = transport.useVirtualThreads();

        threadFactory = new DefaultThreadFactory("replication-updater", false, true)
                .useVirtualThreads(useVirtualThreads);
        if (useVirtualThreads && Util.virtualThreadsAvailable())
            System.out.println("Utilizing virtual threads for benchmark!");

        RAFT raft = channel.getProtocolStack().findProtocol(RAFT.class);
        raft.raftId(cmd.getName());
        raft.addRoleListener(role -> System.out.printf("%s: role is '%s'%n", channel.getAddress(), role));

        disp = new RpcDispatcher(channel, this).setReceiver(this).setMethodLookup(id -> METHODS[id]);

        System.out.printf("Connecting benchmark node to cluster: '%s'%n", CLUSTER_NAME);
        channel.connect(CLUSTER_NAME);
    }

    public final void init() throws Throwable {
        if (channel.getView().getMembers().size() < 2)
            return;

        Address coord = channel.getView().getCoord();
        PerfUtil.Config config = disp.callRemoteMethod(coord, new MethodCall(GET_CONFIG), new RequestOptions(ResponseMode.GET_ALL, 5_000));
        if (config != null) {
            System.out.printf("Fetch config from '%s': %s%n", coord, config);
            for (Map.Entry<String, Object> entry : config.values().entrySet()) {
                Field field = Util.getField(getClass(), entry.getKey());
                Util.setField(field, this, entry.getValue());
            }
        } else {
            System.err.println("Failed to fetch config from " + coord);
        }
    }

    @Override
    public void viewAccepted(View new_view) {
        System.out.printf("Received view: %s%n", new_view);
    }

    public final void eventLoop() throws Throwable {
        while (looping) {
            String message = String.format(BASE_EVENT_LOOP, num_threads, Util.printTime(time, TimeUnit.SECONDS),
                    Util.printTime(timeout, TimeUnit.MILLISECONDS), print_details, print_updaters, benchmark, extendedEventLoopHeader());
            int c = Util.keyPress(message);

            switch (c) {
                case '1':
                    startBenchmark();
                    break;
                case '2':
                    System.out.printf("\n-- local: %s, view: %s\n", channel.getAddress(), channel.getView());
                    break;
                case '4':
                    changeFieldAcrossCluster(NUM_THREADS, Util.readIntFromStdin("Number of updater threads: "));
                    break;
                case '6':
                    changeFieldAcrossCluster(TIME, Util.readIntFromStdin("Time (secs): "));
                    break;
                case 'd':
                    changeFieldAcrossCluster(PRINT_DETAILS, !print_details);
                    break;
                case 'i':
                    changeFieldAcrossCluster(PRINT_INVOKERS, !print_updaters);
                    break;
                case 't':
                    changeFieldAcrossCluster(TIMEOUT, Util.readIntFromStdin("update timeout (ms): "));
                    break;
                case 'b':
                    changeFieldAcrossCluster(BENCHMARK, Util.readStringFromStdin("benchmark mode: "));
                    break;
                case 'v':
                    System.out.printf("Version: %s, Java version: %s\n", Version.printVersion(),
                            System.getProperty("java.vm.version", "n/a"));
                    break;
                case 'x':
                case -1:
                    looping = false;
                    break;
                case 'X':
                    try {
                        RequestOptions options = new RequestOptions(ResponseMode.GET_NONE, 0)
                                .flags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.Flag.NO_FC);
                        disp.callRemoteMethods(null, new MethodCall(QUIT_ALL), options);
                        break;
                    } catch (Throwable t) {
                        System.err.println("Calling quitAll() failed: " + t);
                    }
                    break;
                default:
                    extendedEventLoop(c);
                    break;
            }
        }
    }

    public final void stop() {
        Util.close(disp, channel);
        clear();
    }

    public final UpdateResult startTest() throws Throwable {
        System.out.printf("running for %d seconds\n", time);

        BenchmarkBuilder builder = BenchmarkBuilder.builder()
                .name("raft-benchmark")
                .failurePolicy(Benchmark.FailurePolicy.CANCEL);

        builder.addPlugin(RaftPluginBuilder::new)
                .withStateMachine(new DummyStateMachine())
                .withJChannel(channel);

        PhaseBuilder<?> pb = builder.addPhase(benchmark)
                .constantRate(num_threads)
                .duration(TimeUnit.SECONDS.toMillis(time))
                .maxDuration(TimeUnit.SECONDS.toMillis(time))
                .isWarmup(false);

        pb.scenario()
                .initialSequence("replicate")
                .stepBuilder(createBenchmarkStep())
                .endSequence()
                .endScenario();

        HashMap<String, StatisticsSnapshot> total = new HashMap<>();
        LocalSimulationRunner runner = new LocalSimulationRunner(
                builder.build(),
                (phase, stepId, metric, snapshot, countDown) -> total.computeIfAbsent(phase.name() + "/" + metric, k -> new StatisticsSnapshot()).add(snapshot),
                (phase, min, max) -> System.out.printf("Phase %s used %s - %s sessions.%n", phase, min, max),
                (authority, tag, min, max) -> {});

        try {
            runner.run();
        } catch (Throwable t) {
            t.printStackTrace(System.err);
        }

        StatisticsSnapshot stats = total.values().stream().findFirst().orElseThrow();
        long durationSeconds = (stats.histogram.getEndTimeStamp() - stats.histogram.getStartTimeStamp());
        return new UpdateResult(stats.histogram.getTotalCount(), durationSeconds, stats.histogram);
    }

    public final void quitAll() {
        System.out.println("Received quit all; shutting down");
        stopEventLoop();
        clear();
        System.exit(0);
    }

    public final PerfUtil.Config getConfig() {
        PerfUtil.Config config = new PerfUtil.Config();
        Class<?> clazz = getClass();
        while (clazz != null) {
            for (Field field : Util.getAllDeclaredFieldsWithAnnotations(clazz, Property.class)) {
                if (field.isAnnotationPresent(Property.class)) {
                    config.add(field.getName(), Util.getField(field, this));
                }
            }
            clazz = clazz.getSuperclass();
        }
        return config;
    }

    public final void set(String field_name, Object value) {
        Field field = Util.getField(this.getClass(), field_name);
        if (field == null)
            System.err.println("Field " + field_name + " not found");
        else {
            Util.setField(field, this, value);
            System.out.println(field.getName() + "=" + value);
        }
    }

    private void stopEventLoop() {
        looping = false;
        Util.close(channel);
    }

    private RaftBenchmark getBenchmark(String type) {
        if (type.equals("sync"))
            return syncBenchmark(threadFactory);

        if (type.equals("async"))
            return asyncBenchmark(threadFactory);

        throw new IllegalArgumentException(String.format("Benchmark %s not found!", benchmark));
    }

    /**
     * Creates a new instance of the {@link RaftBenchmark} with synchronous APIs.
     *
     * @param tf: Factory to create threads.
     * @return A new benchmark instance.
     */
    public abstract RaftBenchmark syncBenchmark(ThreadFactory tf);

    /**
     * Creates a new instance of the {@link RaftBenchmark} with asynchronous APIs.
     *
     * @param tf: Factory to create threads.
     * @return A new benchmark instance.
     */
    public abstract RaftBenchmark asyncBenchmark(ThreadFactory tf);

    public abstract StepBuilder<?> createBenchmarkStep();

    /**
     * Expand the event loop with new arguments.
     *
     * @param c: Key pressed.
     * @throws Throwable: If an error occurs while handling the key.
     */
    public void extendedEventLoop(int c) throws Throwable { }

    /**
     * Expand the event loop message.
     *
     * @return Additional properties to add to the event loop message.
     */
    public String extendedEventLoopHeader() {
        return "";
    }

    /**
     * Clear resources created by the benchmark.
     * <p>
     * This method is only invoked when exiting or finishing the benchmark.
     * </p>
     */
    public void clear() { }

    /**
     * Kicks off the benchmark on all cluster nodes
     */
    private void startBenchmark() {
        RspList<UpdateResult> responses;
        try {
            RequestOptions options = new RequestOptions(ResponseMode.GET_ALL, 0)
                    .flags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.Flag.NO_FC);
            responses = disp.callRemoteMethods(null, new MethodCall(START), options);
        } catch (Throwable t) {
            System.err.println("starting the benchmark failed: " + t);
            return;
        }

        long total_incrs = 0;
        long total_time = 0;
        Histogram globalHistogram = null;

        System.out.println("\n======================= Results: ===========================");
        for (Map.Entry<Address, Rsp<UpdateResult>> entry : responses.entrySet()) {
            Address mbr = entry.getKey();
            Rsp<UpdateResult> rsp = entry.getValue();
            UpdateResult result = rsp.getValue();
            if (result != null) {
                total_incrs += result.num_updates;
                total_time += result.total_time;
                if (globalHistogram == null)
                    globalHistogram = result.histogram;
                else
                    globalHistogram.add(result.histogram);
            }
            System.out.println(mbr + ": " + result);
        }
        double total_reqs_sec = total_incrs / (total_time / 1000.0);
        System.out.println("\n");
        System.out.println(Util.bold(String.format("Throughput: %,.2f updates/sec/node\n" +
                        "Time:       %s / update\n",
                total_reqs_sec, print(globalHistogram, print_details))));
        if (print_details && globalHistogram != null) System.out.println(printDetailed(globalHistogram));;
        System.out.println("\n\n");
    }

    protected final void changeFieldAcrossCluster(Field field, Object value) throws Exception {
        disp.callRemoteMethods(null, new MethodCall(SET, field.getName(), value), RequestOptions.SYNC());
    }

    private static String printAverage(long start_time, RaftBenchmark benchmark) {
        long tmp_time = System.currentTimeMillis() - start_time;
        long incrs = benchmark.getTotalUpdates();
        double incrs_sec = incrs / (tmp_time / 1000.0);
        return String.format("%,.0f updates/sec (%,d updates)", incrs_sec, incrs);
    }

    private static String print(AbstractHistogram histogram, boolean details) {
        if (histogram == null) return "no results";

        double avg = histogram.getMean();
        return details ? String.format("min/avg/max = %d/%f/%s", histogram.getMinValue(), avg, histogram.getMaxValue()) :
                String.format("%s", Util.printTime(avg, TimeUnit.NANOSECONDS));
    }

    private static String printDetailed(AbstractHistogram histogram) {
        StringBuilder sb = new StringBuilder();
        sb.append("min/avg/max = ");
        sb.append(Util.printTime(histogram.getMinValue(), TimeUnit.NANOSECONDS)).append('/')
                .append(Util.printTime(histogram.getMean(), TimeUnit.NANOSECONDS)).append('/')
                .append(Util.printTime(histogram.getMaxValue(), TimeUnit.NANOSECONDS));

        sb.append(System.lineSeparator());

        sb.append("p50: ").append(Util.printTime(histogram.getValueAtPercentile(50), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        sb.append("p90: ").append(Util.printTime(histogram.getValueAtPercentile(90), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        sb.append("p99: ").append(Util.printTime(histogram.getValueAtPercentile(99), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        sb.append("p99.9: ").append(Util.printTime(histogram.getValueAtPercentile(99.9), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        sb.append("p99.99: ").append(Util.printTime(histogram.getValueAtPercentile(99.99), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        sb.append(Util.bold("max: ")).append(Util.printTime(histogram.getMaxValue(), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        return sb.toString();
    }

    private void printStats(String phaseAndMetric, StatisticsSnapshot stats) {
        double durationSeconds = (stats.histogram.getEndTimeStamp() - stats.histogram.getStartTimeStamp()) / 1000d;
        System.out.println(phaseAndMetric);
        System.out.printf("%s requests in %s s, %n", stats.histogram.getTotalCount(), durationSeconds);
        System.out.println("                  Avg     Stdev       Max");
        System.out.printf("Latency:    %s %s %s%n", io.hyperfoil.impl.Util.prettyPrintNanosFixed((long) stats.histogram.getMean()),
                io.hyperfoil.impl.Util.prettyPrintNanosFixed((long) stats.histogram.getStdDeviation()),
                io.hyperfoil.impl.Util.prettyPrintNanosFixed(stats.histogram.getMaxValue()));
        System.out.printf("Requests/sec: %s%n", String.format("%.2f", stats.histogram.getTotalCount() / durationSeconds));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        stats.histogram.outputPercentileDistribution(new PrintStream(baos, true, StandardCharsets.UTF_8), 1000.00);
        String data = baos.toString(StandardCharsets.UTF_8);
        System.out.printf("\nPercentile Distribution\n\n%s\n", data);
    }

    public static class UpdateResult implements Streamable {
        protected long num_updates;
        protected long total_time;     // in ms
        protected Histogram histogram; // in ns

        public UpdateResult() { }

        public UpdateResult(long num_updates, long total_time, Histogram histogram) {
            this.num_updates = num_updates;
            this.total_time = total_time;
            this.histogram = histogram;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeLongCompressed(num_updates, out);
            Bits.writeLongCompressed(total_time, out);
            Util.objectToStream(histogram, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            num_updates = Bits.readLongCompressed(in);
            total_time = Bits.readLongCompressed(in);
            histogram = Util.objectFromStream(in);
        }

        @Override
        public String toString() {
            double totalReqsPerSec = num_updates / (total_time / 1000.0);
            return String.format("%,.2f updates/sec (%,d updates, %s / update)\n%s", totalReqsPerSec, num_updates,
                    Util.printTime(histogram.getMean(), TimeUnit.NANOSECONDS), printDetailed(histogram));
        }
    }
}
