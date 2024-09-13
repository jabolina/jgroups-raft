package org.jgroups.perf.harness.hyperfoil;

import org.jgroups.perf.harness.hyperfoil.config.RaftPluginConfig;
import org.jgroups.perf.harness.hyperfoil.internal.RaftHyperfoilBenchmark;

import java.time.Clock;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import io.hyperfoil.api.config.Benchmark;
import io.hyperfoil.api.config.Scenario;
import io.hyperfoil.api.session.Session;
import io.hyperfoil.core.api.PluginRunData;
import io.hyperfoil.core.impl.ConnectionStatsConsumer;
import io.netty.channel.EventLoop;
import io.vertx.core.Future;

public class RaftHyperfoilRunner implements PluginRunData {

    private static final Session.ResourceKey<RaftHyperfoilBenchmark> KEY = new Session.ResourceKey<>() {};
    private final RaftHyperfoilBenchmark[] elements;

    public RaftHyperfoilRunner(Benchmark benchmark, EventLoop[] loops) {
        RaftPluginConfig plugin = benchmark.plugin(RaftPluginConfig.class);

        this.elements = new RaftHyperfoilBenchmark[loops.length];
        for (int i = 0; i < elements.length; i++) {
            elements[i] = new RaftHyperfoilBenchmark(plugin);
        }
    }

    @Override
    public void initSession(Session session, int i, Scenario scenario, Clock clock) {
        RaftHyperfoilBenchmark rhb = elements[i];
        session.declareSingletonResource(KEY, rhb);
    }

    @Override
    public void openConnections(Function<Callable<Void>, Future<Void>> function, Consumer<Future<Void>> consumer) {
        for (RaftHyperfoilBenchmark element : elements) {
            element.start();
        }
    }

    @Override
    public void listConnections(Consumer<String> consumer) { }

    @Override
    public void visitConnectionStats(ConnectionStatsConsumer connectionStatsConsumer) { }

    @Override
    public void shutdown() {
        for (RaftHyperfoilBenchmark element : elements) {
            //element.shutdown();
        }
    }

    public static RaftHyperfoilBenchmark get(Session session) {
        return session.getResource(KEY);
    }
}
