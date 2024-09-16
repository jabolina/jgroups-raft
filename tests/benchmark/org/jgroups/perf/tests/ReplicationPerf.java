package org.jgroups.perf.tests;

import org.jgroups.annotations.Property;
import org.jgroups.perf.CommandLineOptions;
import org.jgroups.perf.harness.AbstractRaftBenchmark;
import org.jgroups.perf.harness.hyperfoil.internal.RaftBenchmarkStepBuilder;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.RaftHandle;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.tests.DummyStateMachine;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test performance of replicating data.
 * <p>
 * This benchmark utilizes the base {@link RaftHandle} to verify the replication performance of a configurable-sized
 * byte array. The test verifies only the replication part, where the state machine does not interpret the bytes.
 * Since the {@link StateMachine} implementation is application-specific, we don't measure it in our tests.
 * </p>
 * <p>
 * The benchmark accepts configuration for the payload size, whether fsync the log.
 * </p>
 *
 * @author José Bolina
 */
public class ReplicationPerf extends AbstractRaftBenchmark {

    private static final Field DATA_SIZE, USE_FSYNC;

    static {
        try {
            DATA_SIZE = Util.getField(ReplicationPerf.class, "data_size", true);
            USE_FSYNC = Util.getField(ReplicationPerf.class, "use_fsync", true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final AtomicReference<byte[]> payload = new AtomicReference<>(null);
    private final RaftHandle raft;

    @Property
    protected int data_size = 526;

    @Property
    protected boolean use_fsync;

    public ReplicationPerf(CommandLineOptions cmd) throws Throwable {
        super(cmd);
        this.raft = new RaftHandle(channel, new DummyStateMachine());
    }

    @Override
    public RaftBenchmarkStepBuilder createBenchmarkStep() {
        visitRaftBeforeBenchmark();
        payload.set(createTestPayload());
        return new RaftBenchmarkStepBuilder().withMethod(this::benchmarkOperation);
    }

    private CompletableFuture<?> benchmarkOperation() {
        byte[] datum = payload.get();
        try {
            return raft.setAsync(datum, 0, datum.length);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public String extendedEventLoopHeader() {
        return String.format("[s] Data size (%d bytes) [f] Use fsync (%b)", data_size, use_fsync);
    }

    @Override
    public void extendedEventLoop(int c) throws Throwable {
        switch (c) {
            case 's':
                changeFieldAcrossCluster(DATA_SIZE, Util.readIntFromStdin("new data size: "));
                break;

            case 'f':
                changeFieldAcrossCluster(USE_FSYNC, !use_fsync);
                break;

            default:
                System.out.printf("Unknown option: %c%n", c);
                break;
        }
    }

    @Override
    public void clear() {
        try {
            RaftTestUtils.deleteRaftLog(channel.getProtocolStack().findProtocol(RAFT.class));
        } catch (Exception e) {
            System.err.printf("Failed deleting log file: %s", e);
        }
    }

    private void visitRaftBeforeBenchmark() {
        raft.raft().logUseFsync(use_fsync);
    }

    private byte[] createTestPayload() {
        byte[] payload = new byte[data_size];
        ThreadLocalRandom.current().nextBytes(payload);
        return payload;
    }
}
