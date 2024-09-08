package org.jgroups.perf.harness.hyperfoil.internal;

import org.jgroups.perf.harness.hyperfoil.config.RaftPluginConfig;
import org.jgroups.raft.RaftHandle;

import io.hyperfoil.api.session.Session;

public class RaftHyperfoilBenchmark implements Session.Resource {

    private final RaftPluginConfig config;
    private RaftHandle handle;

    public RaftHyperfoilBenchmark(RaftPluginConfig config) {
        this.config = config;
    }

    public void start() {
        handle = new RaftHandle(config.channel(), config.stateMachine());
    }

    public void shutdown() {
        if (handle != null)
            handle.channel().close();
    }

    public RaftHandle handle() {
        return handle;
    }
}
