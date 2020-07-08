package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.ShopRow;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.damsel.domain.Suspension;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;

import java.time.LocalDateTime;

public class ShopSuspensionMapper implements Mapper<PartyChange, MachineEvent, ShopRow> {

    @Override
    public ShopRow map(PartyChange change, MachineEvent event) {
        Suspension suspension = change.getShopSuspension().getSuspension();
        LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
        String shopId = change.getShopSuspension().getShopId();
        String partyId = event.getSourceId();

        ShopRow shopRow = new ShopRow();
        shopRow.setEventTime(eventCreatedAt);
        shopRow.setShopdId(shopId);
        shopRow.setPartyId(partyId);
        if (suspension.isSetActive()) {
            shopRow.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(suspension.getActive().getSince()));
        } else if (suspension.isSetSuspended()) {
            shopRow.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(suspension.getSuspended().getSince()));
        }

        return shopRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.SHOP_SUSPENSION;
    }

}
