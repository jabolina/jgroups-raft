package org.jgroups.perf.harness.hyperfoil;

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

    public RaftHyperfoilRunner(Benchmark benchmark, EventLoop[] loops) { }

    @Override
    public void initSession(Session session, int i, Scenario scenario, Clock clock) { }

    @Override
    public void openConnections(Function<Callable<Void>, Future<Void>> function, Consumer<Future<Void>> consumer) {}

    @Override
    public void listConnections(Consumer<String> consumer) { }

    @Override
    public void visitConnectionStats(ConnectionStatsConsumer connectionStatsConsumer) { }

    @Override
    public void shutdown() { }
}
