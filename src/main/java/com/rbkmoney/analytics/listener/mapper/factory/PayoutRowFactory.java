package com.rbkmoney.analytics.listener.mapper.factory;

import com.rbkmoney.analytics.dao.model.PayoutRow;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.payout.manager.Event;
import com.rbkmoney.payout.manager.Payout;
import com.rbkmoney.payout.manager.PayoutCancelled;
import com.rbkmoney.payout.manager.PayoutStatus;
import org.springframework.stereotype.Service;

@Service
public class PayoutRowFactory {

    public PayoutRow create(
            Event event,
            String payoutId,
            PayoutStatus payoutStatus) {
        Payout payout = event.getPayout();
        PayoutRow payoutRow = new PayoutRow();
        payoutRow.setPayoutId(payoutId);
        payoutRow.setPayoutToolId(payout.getPayoutToolId());
        payoutRow.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        payoutRow.setPayoutId(payout.getPayoutId());
        payoutRow.setPartyId(payout.getPartyId());
        payoutRow.setShopId(payout.getShopId());
        payoutRow.setPayoutTime(TypeUtil.stringToLocalDateTime(payout.getCreatedAt()));
        payoutRow.setStatus(TBaseUtil.unionFieldToEnum(payoutStatus,
                com.rbkmoney.analytics.constant.PayoutStatus.class)
        );
        payoutRow.setAmount(payout.getAmount());
        payoutRow.setFee(payout.getFee());
        payoutRow.setCurrency(payout.getCurrency().getSymbolicCode());

        if (payoutStatus.isSetCancelled()) {
            PayoutCancelled cancelled = payoutStatus.getCancelled();
            payoutRow.setStatusCancelledDetails(cancelled.getDetails());
        }

        return payoutRow;
    }
}
