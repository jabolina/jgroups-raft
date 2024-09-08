package org.jgroups.perf.harness.hyperfoil.config;

import org.jgroups.JChannel;
import org.jgroups.raft.StateMachine;

import java.util.Objects;

import io.hyperfoil.api.config.PluginConfig;

public class RaftPluginConfig implements PluginConfig {

    private final StateMachine sm;
    private final JChannel channel;

    public RaftPluginConfig(StateMachine sm, JChannel channel) {
        this.sm = Objects.requireNonNull(sm);
        this.channel = Objects.requireNonNull(channel);
    }

    public StateMachine stateMachine() {
        return sm;
    }

    public JChannel channel() {
        return channel;
    }
}
