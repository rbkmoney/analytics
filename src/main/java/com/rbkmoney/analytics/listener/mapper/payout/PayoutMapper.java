package com.rbkmoney.analytics.listener.mapper.payout;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.analytics.exception.PayoutInfoNotFoundException;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.analytics.listener.mapper.factory.PayoutRowFactory;
import com.rbkmoney.analytics.service.PayouterClientService;
import com.rbkmoney.damsel.payout_processing.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutMapper implements Mapper<PayoutChange, Event, PayoutRow> {

    private final PayouterClientService payouterClientService;
    private final PayoutRowFactory payoutRowFactory;

    @Override
    public PayoutRow map(PayoutChange change, Event event) {
        Long eventId = event.getId();
        String payoutId = event.getSource().getPayoutId();
        PayoutStatus payoutStatus = change.getPayoutStatusChanged().getStatus();

        List<Event> events = payouterClientService.getEvents(payoutId, eventId);

        Payout payoutCreated = events.stream()
                .flatMap(e -> e.getPayload().getPayoutChanges().stream())
                .filter(PayoutChange::isSetPayoutCreated)
                .map(PayoutChange::getPayoutCreated)
                .map(PayoutCreated::getPayout)
                .findFirst()
                .orElseThrow(() -> new PayoutInfoNotFoundException(payoutId));
        PayoutRow payoutRow = payoutRowFactory.create(
                event,
                payoutCreated,
                payoutId,
                payoutStatus);

        if (payoutStatus.isSetCancelled()) {
            Optional<PayoutStatus> payoutPaid = events.stream()
                    .flatMap(e -> e.getPayload().getPayoutChanges().stream())
                    .filter(PayoutChange::isSetPayoutStatusChanged)
                    .map(PayoutChange::getPayoutStatusChanged)
                    .map(PayoutStatusChanged::getStatus)
                    .filter(PayoutStatus::isSetPaid)
                    .findFirst();

            if (payoutPaid.isPresent()) {
                payoutRow.setCancelledAfterBeingPaid(true);
            }
        }

        return payoutRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.PAYOUT_STATUS_CHANGED;
    }
}
