package org.jgroups.perf.harness.hyperfoil.internal;

import java.util.Arrays;
import java.util.List;

import io.hyperfoil.api.config.Locator;
import io.hyperfoil.api.config.Name;
import io.hyperfoil.api.config.Step;
import io.hyperfoil.api.config.StepBuilder;
import io.hyperfoil.api.session.Session;
import io.hyperfoil.core.builders.BaseStepBuilder;
import io.hyperfoil.core.generators.StringGeneratorBuilder;
import io.hyperfoil.core.metric.MetricSelector;
import io.hyperfoil.core.metric.ProvidedMetricSelector;
import io.hyperfoil.core.steps.StatisticsStep;
import io.hyperfoil.function.SerializableFunction;
import org.kohsuke.MetaInfServices;

@MetaInfServices(StepBuilder.class)
@Name("jgroups-raft")
public class RaftBenchmarkStepBuilder extends BaseStepBuilder<RaftBenchmarkStepBuilder> {

    private MetricSelector metricSelector;
    private StringGeneratorBuilder payloadGenerator;

    @Override
    public void prepareBuild() {
        if (metricSelector == null) {
            String sequenceName = Locator.current().sequence().name();
            metricSelector = new ProvidedMetricSelector(sequenceName);
        }
    }

    @Override
    public List<Step> build() {
        int stepId = StatisticsStep.nextId();
        RaftOperationResource.Key key = new RaftOperationResource.Key();
        SerializableFunction<Session, byte[]> generator = new SerializableFunction<Session, byte[]>() {
            @Override
            public byte[] apply(Session session) {
                if (payloadGenerator == null) return null;
                SerializableFunction<Session, String> delegate = payloadGenerator.build();
                return delegate.apply(session).getBytes();
            }
        };
        RaftBenchmarkStep request = new RaftBenchmarkStep(stepId, key, metricSelector, generator);
        RaftResponseStep response = new RaftResponseStep(key);
        return Arrays.asList(request, response);
    }
}
