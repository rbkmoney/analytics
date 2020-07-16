package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.EventType;

public interface ChangeHandler<C, P, R>  {

    default boolean accept(C change) {
        return getChangeType().getFilter().match(change);
    }

    void handleChange(C change, P parent, LocalStorage<R> localStorage);

    EventType getChangeType();

}
