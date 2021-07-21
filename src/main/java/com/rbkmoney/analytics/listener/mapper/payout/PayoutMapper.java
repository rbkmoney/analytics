package com.rbkmoney.analytics.listener.mapper.payout;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePayoutRepository;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.analytics.listener.mapper.factory.PayoutRowFactory;
import com.rbkmoney.payout.manager.Event;
import com.rbkmoney.payout.manager.PayoutChange;
import com.rbkmoney.payout.manager.PayoutStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutMapper implements Mapper<PayoutChange, Event, PayoutRow> {

    private final PayoutRowFactory payoutRowFactory;
    private final ClickHousePayoutRepository clickHousePayoutRepository;

    @Override
    public PayoutRow map(PayoutChange change, Event event) {
        String payoutId = event.getPayoutId();
        PayoutStatus payoutStatus = change.getStatusChanged().getStatus();
        PayoutRow payoutRow = payoutRowFactory.create(event, payoutId, payoutStatus);
        if (payoutStatus.isSetCancelled()) {
            String paidEvent = clickHousePayoutRepository.getPaidEvent(payoutId);
            payoutRow.setCancelledAfterBeingPaid(paidEvent != null);
        }
        return payoutRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.PAYOUT_STATUS_CHANGED;
    }
}
