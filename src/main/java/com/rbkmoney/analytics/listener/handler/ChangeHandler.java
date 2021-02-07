package com.rbkmoney.analytics.listener.handler;

import com.rbkmoney.analytics.constant.EventType;

public interface ChangeHandler<C, P>  {

    default boolean accept(C change) {
        return getChangeType().getFilter().match(change);
    }

    void handleChange(C change, P parent);

    EventType getChangeType();

}
