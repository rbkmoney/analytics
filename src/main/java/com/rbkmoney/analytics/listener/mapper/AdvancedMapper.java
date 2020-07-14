package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.EventType;

public interface AdvancedMapper<C, P, R>  {

    default boolean accept(C change) {
        return getChangeType().getFilter().match(change);
    }

    R map(C change, P parent, LocalStorage<R> localStorage);

    EventType getChangeType();

}
