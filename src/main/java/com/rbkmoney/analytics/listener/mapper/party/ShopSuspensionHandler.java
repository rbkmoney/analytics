package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.damsel.domain.Suspension;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ShopSuspensionHandler implements ChangeHandler<PartyChange, MachineEvent, List<Shop>> {

    public List<Shop> handleChange(PartyChange change, MachineEvent event) {
        Suspension suspension = change.getShopSuspension().getSuspension();
        String shopId = change.getShopSuspension().getShopId();
        String partyId = event.getSourceId();

        Shop shop = new Shop();
        shop.setPartyId(partyId);
        shop.setShopId(shopId);
        shop.setEventId(event.getEventId());
        shop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        if (suspension.isSetActive()) {
            shop.setSuspension(com.rbkmoney.analytics.domain.db.enums.Suspension.active);
            shop.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(suspension.getActive().getSince()));
        } else if (suspension.isSetSuspended()) {
            shop.setSuspension(com.rbkmoney.analytics.domain.db.enums.Suspension.suspended);
            shop.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(suspension.getSuspended().getSince()));
        }

        return List.of(shop);
    }

    @Override
    public EventType getChangeType() {
        return EventType.SHOP_SUSPENSION;
    }

}
