package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.AdvancedMapper;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.domain.Suspension;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ShopSuspensionMapper implements AdvancedMapper<PartyChange, MachineEvent, Shop> {

    private final PartyService partyService;

    @Override
    public Shop map(PartyChange change, MachineEvent event, LocalStorage<Shop> storage) {
        Suspension suspension = change.getShopSuspension().getSuspension();
        String shopId = change.getShopSuspension().getShopId();
        String partyId = event.getSourceId();

        Shop shop = partyService.getShop(partyId, shopId, storage);
        if (suspension.isSetActive()) {
            shop.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(suspension.getActive().getSince()));
        } else if (suspension.isSetSuspended()) {
            shop.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(suspension.getSuspended().getSince()));
        }

        return shop;
    }

    @Override
    public EventType getChangeType() {
        return EventType.SHOP_SUSPENSION;
    }

}
