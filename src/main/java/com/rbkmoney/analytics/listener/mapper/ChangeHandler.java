package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.listener.handler.party.LocalStorage;

public interface ChangeHandler<C, P>  {

    default boolean accept(C change) {
        return getChangeType().getFilter().match(change);
    }

    void handleChange(C change, P parent, LocalStorage localStorage);

    EventType getChangeType();

}
