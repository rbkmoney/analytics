package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.BlockingType;
import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.ShopRow;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.damsel.domain.Blocking;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;

import java.time.LocalDateTime;

public class ShopBlockingMapper implements Mapper<PartyChange, MachineEvent, ShopRow> {

    @Override
    public ShopRow map(PartyChange change, MachineEvent event) {
        Blocking blocking = change.getShopBlocking().getBlocking();
        LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
        String shopId = change.getShopBlocking().getShopId();
        String partyId = event.getSourceId();

        ShopRow shopRow = new ShopRow();
        shopRow.setEventTime(eventCreatedAt);
        shopRow.setShopdId(shopId);
        shopRow.setPartyId(partyId);
        shopRow.setBlocking(TBaseUtil.unionFieldToEnum(change.getShopBlocking().getBlocking(), BlockingType.class));
        if (blocking.isSetUnblocked()) {
            shopRow.setUnblockedReason(blocking.getUnblocked().getReason());
            shopRow.setUnblockedSince(TypeUtil.stringToLocalDateTime(blocking.getUnblocked().getSince()));
        } else if (blocking.isSetBlocked()) {
            shopRow.setBlockedReason(blocking.getBlocked().getReason());
            shopRow.setBlockedSince(TypeUtil.stringToLocalDateTime(blocking.getBlocked().getSince()));
        }

        return shopRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.SHOP_BLOCKING;
    }
}
