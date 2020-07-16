package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.domain.Blocking;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class ShopBlockingHandler implements ChangeHandler<PartyChange, MachineEvent, Shop> {

    private final PartyService partyService;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event, LocalStorage<Shop> storage) {
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

        partyService.saveShop(shop);
        storage.put(partyId + shopId, shop);
    }

    @Override
    public EventType getChangeType() {
        return EventType.SHOP_BLOCKING;
    }
}
