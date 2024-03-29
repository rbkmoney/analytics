package com.rbkmoney.analytics.utils;


import com.rbkmoney.damsel.payment_processing.EventRange;
import org.springframework.stereotype.Service;

@Service
public class EventRangeFactory {

    public EventRange create(long eventNumber) {
        return new EventRange()
                .setLimit((int) eventNumber);
    }

}
