package com.rbkmoney.analytics.listener.mapper;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.damsel.payout_processing.Event;
import com.rbkmoney.damsel.payout_processing.PayoutChange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutMapper implements Mapper<PayoutChange, Event, PayoutRow> {

    @Override
    public PayoutRow map(PayoutChange change, Event parent) {
        // TODO [a.romanov]: impl
        throw new UnsupportedOperationException();
    }

    @Override
    public EventType getChangeType() {
        // TODO [a.romanov]: impl
        throw new UnsupportedOperationException();
    }
}
