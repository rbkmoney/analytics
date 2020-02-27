package com.rbkmoney.analytics.listener.handler;

import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.mapper.Mapper;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface BatchHandler<C, P> {

    default boolean accept(C change) {
        return getMappers().stream().anyMatch(mapper -> mapper.accept(change));
    }

    Processor handle(List<Map.Entry<P, C>> changes);

    <T> List<Mapper<C, P, T>> getMappers();

}
