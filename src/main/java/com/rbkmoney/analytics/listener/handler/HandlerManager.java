package com.rbkmoney.analytics.listener.handler;

import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class HandlerManager<C, P> {

    private final List<BatchHandler<C, P>> handlers;

    private final List<AdvancedBatchHandler<C, P>> advancedBatchHandlers;

    public BatchHandler<C, P> getHandler(C change) {
        for (BatchHandler<C, P> handler : handlers) {
            if (handler.accept(change)) {
                return handler;
            }
        }
        return null;
    }

    public AdvancedBatchHandler<C, P> getAdvancedHandler(C change) {
        for (AdvancedBatchHandler<C, P> advancedBatchHandler : advancedBatchHandlers) {
            if (advancedBatchHandler.accept(change)) {
                return advancedBatchHandler;
            }
        }
        return null;
    }

}
