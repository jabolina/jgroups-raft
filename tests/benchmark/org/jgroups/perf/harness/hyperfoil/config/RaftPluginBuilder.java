package org.jgroups.perf.harness.hyperfoil.config;

import org.jgroups.JChannel;
import org.jgroups.raft.StateMachine;

import io.hyperfoil.api.config.BenchmarkBuilder;
import io.hyperfoil.api.config.BuilderBase;
import io.hyperfoil.api.config.PluginBuilder;
import io.hyperfoil.api.config.PluginConfig;

public class RaftPluginBuilder extends PluginBuilder<RaftPluginBuilder.RaftErgonomics> {
    private final RaftBenchmarkBuilder builder;
    private final RaftErgonomics ergonomics;

    public RaftPluginBuilder(BenchmarkBuilder parent) {
        super(parent);
        this.ergonomics = new RaftErgonomics(this);
        this.builder = new RaftBenchmarkBuilder();
    }

    @Override
    public RaftErgonomics ergonomics() {
        return ergonomics;
    }

    @Override
    public void prepareBuild() {
        builder.prepareBuild();
    }

    @Override
    public PluginConfig build() {
        return new RaftPluginConfig(builder.stateMachine, builder.channel);
    }

    public RaftPluginBuilder withStateMachine(StateMachine sm) {
        builder.stateMachine = sm;
        return this;
    }

    public RaftPluginBuilder withJChannel(JChannel channel) {
        builder.channel = channel;
        return this;
    }

    public static class RaftErgonomics {
        private final RaftPluginBuilder parent;

        public RaftErgonomics(RaftPluginBuilder parent) {
            this.parent = parent;
        }
    }

    public static final class RaftBenchmarkBuilder implements BuilderBase<RaftBenchmarkBuilder> {
        private StateMachine stateMachine;
        private JChannel channel;
    }
}
