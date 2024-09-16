package org.jgroups.perf.harness.hyperfoil.internal;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import io.hyperfoil.api.config.Locator;
import io.hyperfoil.api.config.Name;
import io.hyperfoil.api.config.Step;
import io.hyperfoil.api.config.StepBuilder;
import io.hyperfoil.core.builders.BaseStepBuilder;
import io.hyperfoil.core.metric.MetricSelector;
import io.hyperfoil.core.metric.ProvidedMetricSelector;
import io.hyperfoil.core.steps.StatisticsStep;
import org.kohsuke.MetaInfServices;

@MetaInfServices(StepBuilder.class)
@Name("jgroups-raft")
public class RaftBenchmarkStepBuilder extends BaseStepBuilder<RaftBenchmarkStepBuilder> {

    private MetricSelector metricSelector;
    private Supplier<CompletableFuture<?>> method = null;

    @Override
    public void prepareBuild() {
        if (metricSelector == null) {
            String sequenceName = Locator.current().sequence().name();
            metricSelector = new ProvidedMetricSelector(sequenceName);
        }
    }

    public RaftBenchmarkStepBuilder withMethod(Supplier<CompletableFuture<?>> method) {
        this.method = method;
        return this;
    }

    @Override
    public List<Step> build() {
        int stepId = StatisticsStep.nextId();
        RaftOperationResource.Key key = new RaftOperationResource.Key();
        RaftRequestStep request = new RaftRequestStep(stepId, key, metricSelector, method);
        RaftResponseStep response = new RaftResponseStep(key);
        return Arrays.asList(request, response);
    }
}
