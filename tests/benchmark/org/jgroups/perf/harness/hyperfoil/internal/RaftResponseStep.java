package org.jgroups.perf.harness.hyperfoil.internal;

import io.hyperfoil.api.config.Step;
import io.hyperfoil.api.session.Session;

public class RaftResponseStep implements Step {

    private final RaftOperationResource.Key key;

    public RaftResponseStep(RaftOperationResource.Key key) {
        this.key = key;
    }

    @Override
    public boolean invoke(Session session) {
        RaftOperationResource operation = session.getResource(key);
        return operation.isComplete();
    }
}
