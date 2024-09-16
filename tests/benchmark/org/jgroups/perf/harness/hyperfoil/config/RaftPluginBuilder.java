package org.jgroups.perf.harness.hyperfoil.config;

import io.hyperfoil.api.config.BenchmarkBuilder;
import io.hyperfoil.api.config.PluginBuilder;
import io.hyperfoil.api.config.PluginConfig;

public class RaftPluginBuilder extends PluginBuilder<RaftPluginBuilder.RaftErgonomics> {
    private final RaftErgonomics ergonomics;

    public RaftPluginBuilder(BenchmarkBuilder parent) {
        super(parent);
        this.ergonomics = new RaftErgonomics();
    }

    @Override
    public RaftErgonomics ergonomics() {
        return ergonomics;
    }

    @Override
    public void prepareBuild() { }

    @Override
    public PluginConfig build() {
        return new RaftPlugin.RaftPluginConfig();
    }

    public static class RaftErgonomics { }
}
