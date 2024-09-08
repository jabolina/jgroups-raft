package org.jgroups.perf.harness.hyperfoil.internal;

import java.util.concurrent.CompletableFuture;

import io.hyperfoil.api.session.Session;

public class RaftOperationResource implements Session.Resource {

    private long startNs;
    private long startMs;
    private CompletableFuture<?> cf;

    public void set(CompletableFuture<?> cf, long startNs, long startMs) {
        this.startNs = startNs;
        this.startMs = startMs;
        this.cf = cf;
    }

    public boolean isComplete() {
        return cf.isDone();
    }

    public long getStartNs() {
        return startNs;
    }

    public long getStartMs() {
        return startMs;
    }

    public static class Key implements Session.ResourceKey<RaftOperationResource> { }
}
