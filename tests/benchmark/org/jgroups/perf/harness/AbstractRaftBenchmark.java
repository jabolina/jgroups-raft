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
import org.jgroups.perf.harness.hyperfoil.internal.RaftBenchmarkStepBuilder;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.tests.perf.PerfUtil;
import org.jgroups.util.Bits;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.hyperfoil.api.config.Benchmark;
import io.hyperfoil.api.config.BenchmarkBuilder;
import io.hyperfoil.api.config.PhaseBuilder;
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
 *     <li>{@link #extendedEventLoop(int)}: Extend the event loop to parse additional options;</li>
 *     <li>{@link #extendedEventLoopHeader()}: Extend the event loop options message;</li>
 *     <li>{@link #clear()}: Clear resources created by the benchmark at exit.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Most importantly, the harness is backed by Hyperfoil [1]. Therefore, subclasses must provide a step to run with
 * Hyperfoil. The clustering will start the benchmark on every node, triggering a local execution of the benchmark.
 * </p>
 *
 * @author José Bolina
 * @see <a href="https://hyperfoil.io">[1] Hyperfoil</a>
 */
public abstract class AbstractRaftBenchmark implements Receiver {

    protected static final Field REQ_PER_SEC, TIME, PRINT_DETAILS;
    private static final Method[] METHODS = new Method[4];
    private static final short START = 0;
    private static final short GET_CONFIG = 1;
    private static final short SET = 2;
    private static final short QUIT_ALL = 3;
    private static final String CLUSTER_NAME;
    private static final String BASE_EVENT_LOOP =
            "[1] Start test [2] View [4] Req/s (%d) [6] Time (%s)" +
                    "\n[d] details (%b)  [v] Version" +
                    "\n%s" +
                    "\n[x] Exit [X] Exit all";

    static {
        try {
            METHODS[START] = AbstractRaftBenchmark.class.getMethod("startTest");
            METHODS[GET_CONFIG] = AbstractRaftBenchmark.class.getMethod("getConfig");
            METHODS[SET] = AbstractRaftBenchmark.class.getMethod("set", String.class, Object.class);
            METHODS[QUIT_ALL] = AbstractRaftBenchmark.class.getMethod("quitAll");

            REQ_PER_SEC = Util.getField(AbstractRaftBenchmark.class, "req_per_sec", true);
            TIME = Util.getField(AbstractRaftBenchmark.class, "time", true);
            PRINT_DETAILS = Util.getField(AbstractRaftBenchmark.class, "print_details", true);

            PerfUtil.init();
            ClassConfigurator.addIfAbsent((short) 1050, UpdateResult.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        CLUSTER_NAME = MethodHandles.lookup().lookupClass().getSimpleName();
    }

    protected final JChannel channel;
    private final String histogramPath;
    private final RpcDispatcher disp;

    @Property
    protected int req_per_sec = 100;

    @Property
    protected int time = 30; // in seconds

    @Property
    protected boolean print_details;

    private volatile boolean looping = true;

    public AbstractRaftBenchmark(CommandLineOptions cmd) throws Throwable {
        this.histogramPath = cmd.getHistogramPath();
        if (histogramPath != null)
            System.out.printf("Histogram enabled! Storing in '%s'%n", histogramPath);

        this.channel = new JChannel(cmd.getProps()).name(cmd.getName());
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
            String message = String.format(BASE_EVENT_LOOP, req_per_sec, Util.printTime(time, TimeUnit.SECONDS),
                    print_details, extendedEventLoopHeader());
            int c = Util.keyPress(message);

            switch (c) {
                case '1':
                    startBenchmark();
                    break;
                case '2':
                    System.out.printf("\n-- local: %s, view: %s\n", channel.getAddress(), channel.getView());
                    break;
                case '4':
                    changeFieldAcrossCluster(REQ_PER_SEC, Util.readIntFromStdin("Number of req/sec/node: "));
                    break;
                case '6':
                    changeFieldAcrossCluster(TIME, Util.readIntFromStdin("Time (secs): "));
                    break;
                case 'd':
                    changeFieldAcrossCluster(PRINT_DETAILS, !print_details);
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

    public UpdateResult startTest() {
        System.out.printf("running for %d seconds\n", time);

        BenchmarkBuilder builder = BenchmarkBuilder.builder()
                .name("raft-benchmark")
                .threads(Runtime.getRuntime().availableProcessors())
                .failurePolicy(Benchmark.FailurePolicy.CANCEL);

        builder.addPlugin(RaftPluginBuilder::new);

        PhaseBuilder<?> pb = builder.addPhase("benchmark")
                .constantRate(req_per_sec)
                .duration(TimeUnit.SECONDS.toMillis(time))
                .maxDuration(TimeUnit.SECONDS.toMillis(time))
                .isWarmup(false);

        pb.scenario()
                .initialSequence(this.getClass().getSimpleName())
                .stepBuilder(createBenchmarkStep())
                .endSequence()
                .endScenario();

        final StatisticsSnapshot stats = new StatisticsSnapshot();
        try {
            Benchmark b = builder.build();
            LocalSimulationRunner runner = new LocalSimulationRunner(
                    b,
                    (phase, stepId, metric, snapshot, countDown) -> stats.add(snapshot),
                    (phase, min, max) -> System.out.printf("Phase %s used %s - %s sessions.%n", phase, min, max),
                    (authority, tag, min, max) -> {});
            runner.run();
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            throw t;
        }

        if (histogramPath != null) {
            String fileName = String.format("histogram_%s.hgrm", channel.getName());
            Path filePath = Path.of(histogramPath, fileName);

            System.out.printf("Storing histogram to '%s'%n", filePath.toAbsolutePath());
            try {
                HistogramUtil.writeTo(stats.histogram, filePath.toFile());
            } catch (IOException e) {
                System.err.printf("Failed writing histogram: %s", e);
            }
        }

        long durationMs = (stats.histogram.getEndTimeStamp() - stats.histogram.getStartTimeStamp());
        System.out.printf("Run for %d sec%n", durationMs);

        return new UpdateResult(stats.histogram.getTotalCount(), stats.errors(), durationMs, stats.histogram);
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

    /**
     * Creates a step to run with Hyperfoil.
     *
     * @return An instance of {@link RaftBenchmarkStepBuilder} with the method to benchmark.
     */
    public abstract RaftBenchmarkStepBuilder createBenchmarkStep();

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

    private static String print(AbstractHistogram histogram, boolean details) {
        if (histogram == null) return "no results";

        double avg = histogram.getMean();
        return details
                ? String.format("min/avg/max = %s/%s/%s",
                    Util.printTime(histogram.getMinValue(), TimeUnit.NANOSECONDS),
                    Util.printTime(avg, TimeUnit.NANOSECONDS),
                    Util.printTime(histogram.getMaxValue(), TimeUnit.NANOSECONDS))
                : String.format("%s", Util.printTime(avg, TimeUnit.NANOSECONDS));
    }

    private static String printDetailed(AbstractHistogram histogram) {
        StringBuilder sb = new StringBuilder();
        sb.append("p50:    ").append(Util.printTime(histogram.getValueAtPercentile(50), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        sb.append("p90:    ").append(Util.printTime(histogram.getValueAtPercentile(90), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        sb.append("p99:    ").append(Util.printTime(histogram.getValueAtPercentile(99), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        sb.append("p99.9:  ").append(Util.printTime(histogram.getValueAtPercentile(99.9), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        sb.append("p99.99: ").append(Util.printTime(histogram.getValueAtPercentile(99.99), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        sb.append(Util.bold("max:    ")).append(Util.printTime(histogram.getMaxValue(), TimeUnit.NANOSECONDS)).append(System.lineSeparator());
        return sb.toString();
    }

    public static class UpdateResult implements Streamable {
        protected long num_updates;
        private long errors;
        protected long total_time;     // in ms
        protected Histogram histogram; // in ns

        public UpdateResult() { }

        public UpdateResult(long num_updates, long errors, long total_time, Histogram histogram) {
            this.num_updates = num_updates;
            this.errors = errors;
            this.total_time = total_time;
            this.histogram = histogram;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeLongCompressed(num_updates, out);
            Bits.writeLongCompressed(errors, out);
            Bits.writeLongCompressed(total_time, out);
            Util.objectToStream(histogram, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            num_updates = Bits.readLongCompressed(in);
            errors = Bits.readLongCompressed(in);
            total_time = Bits.readLongCompressed(in);
            histogram = Util.objectFromStream(in);
        }

        @Override
        public String toString() {
            double totalReqsPerSec = num_updates / (total_time / 1000.0);
            return String.format("%,.2f updates/sec (%,d updates, %s / update, %d errors)\n%s", totalReqsPerSec, num_updates,
                    Util.printTime(histogram.getMean(), TimeUnit.NANOSECONDS), errors, printDetailed(histogram));
        }
    }
}
