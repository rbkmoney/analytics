package com.rbkmoney.analytics.listener.handler.dominant;

import com.rbkmoney.damsel.domain_config.Operation;

public interface DominantHandler {

    void handle(Operation operation, long versionId);

    boolean isHandle(Operation change);

}
