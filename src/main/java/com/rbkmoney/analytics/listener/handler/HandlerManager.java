package com.rbkmoney.analytics.listener.handler;

import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class HandlerManager<C, P> {

    private final List<BatchHandler<C, P>> handlers;

    public BatchHandler<C, P> getHandler(C change) {
        for (BatchHandler<C, P> handler : handlers) {
            if (handler.accept(change)) {
                return handler;
            }
        }
        return null;
    }

}
