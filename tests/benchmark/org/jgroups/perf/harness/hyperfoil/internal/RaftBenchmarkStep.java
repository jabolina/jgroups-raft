package org.jgroups.perf.harness.hyperfoil.internal;

import org.jgroups.perf.harness.hyperfoil.RaftHyperfoilRunner;
import org.jgroups.raft.Options;
import org.jgroups.raft.RaftHandle;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import io.hyperfoil.api.config.SLA;
import io.hyperfoil.api.session.ResourceUtilizer;
import io.hyperfoil.api.session.Session;
import io.hyperfoil.api.statistics.Statistics;
import io.hyperfoil.core.metric.MetricSelector;
import io.hyperfoil.core.steps.StatisticsStep;
import io.hyperfoil.function.SerializableFunction;

public class RaftBenchmarkStep extends StatisticsStep implements ResourceUtilizer, SLA.Provider {

    private final SerializableFunction<Session, byte[]> payloadGenerator;
    private final MetricSelector metricSelector;
    private final RaftOperationResource.Key operationKey;

    protected RaftBenchmarkStep(int id,
                                RaftOperationResource.Key operationKey,
                                MetricSelector metricSelector,
                                SerializableFunction<Session, byte[]> payloadGenerator) {
        super(id);
        this.metricSelector = metricSelector;
        this.operationKey = operationKey;
        this.payloadGenerator = payloadGenerator;
    }

    @Override
    public SLA[] sla() {
        return new SLA[0];
    }

    @Override
    public boolean invoke(Session session) {
        byte[] payload = payloadGenerator.apply(session);

        String metric = metricSelector.apply(null, "");
        Statistics statistics = session.statistics(id(), metric);
        RaftHyperfoilBenchmark benchmark = RaftHyperfoilRunner.get(session);
        RaftHandle handle = benchmark.handle();

        long startTimestampMs = System.currentTimeMillis();
        long startTimestampNanos = System.nanoTime();
        CompletableFuture<?> cf;
        try {
            cf = handle.setAsync(payload, 0, payload.length, Options.DEFAULT_OPTIONS);
        } catch (Exception e) {
            cf = CompletableFuture.failedFuture(e);
        }

        statistics.incrementRequests(startTimestampMs);
        cf.exceptionally(t -> {
            trackResponseError(session, metric, t);
            return null;
        });
        cf.thenRun(() -> {
            trackResponseSuccess(session, metric);
            session.proceed();
        });
        session.getResource(operationKey).set(cf, startTimestampNanos, startTimestampMs);
        return true;
    }

    @Override
    public void reserve(Session session) {
        session.declareResource(operationKey, RaftOperationResource::new);
    }

    private void trackResponseError(Session session, String metric, Throwable t) {
        Statistics statistics = session.statistics(id(), metric);
        if (t instanceof TimeoutException) {
            statistics.incrementTimeouts(System.currentTimeMillis());
        } else {
            statistics.incrementInternalErrors(System.currentTimeMillis());
        }

        session.stop();
    }

    private void trackResponseSuccess(Session session, String metric) {
        long endNs = System.nanoTime();

        RaftOperationResource resource = session.getResource(operationKey);
        long startNs = resource.getStartNs();
        long startMs = resource.getStartMs();

        Statistics statistics = session.statistics(id(), metric);
        statistics.recordResponse(startMs, endNs - startNs);
    }
}
