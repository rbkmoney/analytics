package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.AdvancedMapper;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.domain.Blocking;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ShopBlockingMapper implements AdvancedMapper<PartyChange, MachineEvent, Shop> {

    private final PartyService partyService;

    @Override
    public Shop map(PartyChange change, MachineEvent event, LocalStorage<Shop> storage) {
        Blocking blocking = change.getShopBlocking().getBlocking();
        String shopId = change.getShopBlocking().getShopId();
        String partyId = event.getSourceId();

        Shop shop = partyService.getShop(partyId, shopId, storage);
        shop.setBlocking(TBaseUtil.unionFieldToEnum(change.getShopBlocking().getBlocking(), com.rbkmoney.analytics.domain.db.enums.Blocking.class));
        if (blocking.isSetUnblocked()) {
            shop.setUnblockedReason(blocking.getUnblocked().getReason());
            shop.setUnblockedSince(TypeUtil.stringToLocalDateTime(blocking.getUnblocked().getSince()));
        } else if (blocking.isSetBlocked()) {
            shop.setBlockedReason(blocking.getBlocked().getReason());
            shop.setBlockedSince(TypeUtil.stringToLocalDateTime(blocking.getBlocked().getSince()));
        }

        return shop;
    }

    @Override
    public EventType getChangeType() {
        return EventType.SHOP_BLOCKING;
    }
}
