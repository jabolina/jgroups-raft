package org.jgroups.perf.harness.hyperfoil.config;

import org.jgroups.perf.harness.hyperfoil.RaftHyperfoilRunner;

import io.hyperfoil.api.config.Benchmark;
import io.hyperfoil.api.config.BenchmarkBuilder;
import io.hyperfoil.api.config.PluginConfig;
import io.hyperfoil.core.api.Plugin;
import io.hyperfoil.core.api.PluginRunData;
import io.hyperfoil.core.parser.ErgonomicsParser;
import io.hyperfoil.core.parser.Parser;
import io.netty.channel.EventLoop;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Plugin.class)
public class RaftPlugin implements Plugin {

    @Override
    public Class<? extends PluginConfig> configClass() {
        return RaftPluginConfig.class;
    }

    @Override
    public String name() {
        return "jgroups-raft";
    }

    @Override
    public Parser<BenchmarkBuilder> parser() {
        throw new IllegalStateException("JGroups Raft does not have a parser");
    }

    @Override
    public void enhanceErgonomics(ErgonomicsParser ergonomicsParser) { }

    @Override
    public PluginRunData createRunData(Benchmark benchmark, EventLoop[] eventLoops, int i) {
        return new RaftHyperfoilRunner(benchmark, eventLoops);
    }
}
