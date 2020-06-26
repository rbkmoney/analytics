package com.rbkmoney.analytics.listener.mapper.payout;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.analytics.service.PayouterClientService;
import com.rbkmoney.damsel.payout_processing.Event;
import com.rbkmoney.damsel.payout_processing.PayoutChange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutMapper implements Mapper<PayoutChange, Event, PayoutRow> {

    private final PayouterClientService payouterClientService;

    @Override
    public PayoutRow map(PayoutChange change, Event parent) {
        Long eventId = parent.getId();
        String payoutId = parent.getSource().getPayoutId();

        List<Event> events = payouterClientService.getEvents(payoutId, eventId);

        // TODO [a.romanov]: impl
        throw new UnsupportedOperationException();
    }

    @Override
    public EventType getChangeType() {
        return EventType.PAYOUT_STATUS_CHANGED;
    }
}
