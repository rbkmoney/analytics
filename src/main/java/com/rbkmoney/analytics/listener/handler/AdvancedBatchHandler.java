package com.rbkmoney.analytics.listener.handler;

import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.mapper.AdvancedMapper;

import java.util.List;
import java.util.Map;

public interface AdvancedBatchHandler<C, P> {
    default boolean accept(C change) {
        return getMappers().stream().anyMatch(mapper -> mapper.accept(change));
    }

    Processor handle(List<Map.Entry<P, C>> changes);

    <T> List<AdvancedMapper<C, P, T>> getMappers();
}
