package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.EventType;

public interface ChangeHandler<C, P, T>  {

    default boolean accept(C change) {
        return getChangeType().getFilter().match(change);
    }

    T handleChange(C change, P parent);

    EventType getChangeType();

}
