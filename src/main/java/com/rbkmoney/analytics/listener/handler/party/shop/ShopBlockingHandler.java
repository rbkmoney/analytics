package com.rbkmoney.analytics.listener.handler.party.shop;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ShopDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.handler.ChangeHandler;
import com.rbkmoney.analytics.listener.handler.merger.ShopEventMerger;
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
public class ShopBlockingHandler implements ChangeHandler<PartyChange, MachineEvent> {

    private final ShopEventMerger shopEventMerger;
    private final ShopDao shopDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        String shopId = change.getShopBlocking().getShopId();
        String partyId = event.getSourceId();

        Shop shop = new Shop();
        shop.setPartyId(partyId);
        shop.setShopId(shopId);
        shop.setEventId(event.getEventId());
        shop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        shop.setBlocking(TBaseUtil.unionFieldToEnum(change.getShopBlocking().getBlocking(),
                com.rbkmoney.analytics.domain.db.enums.Blocking.class));
        Blocking blocking = change.getShopBlocking().getBlocking();
        if (blocking.isSetUnblocked()) {
            shop.setUnblockedReason(blocking.getUnblocked().getReason());
            shop.setUnblockedSince(TypeUtil.stringToLocalDateTime(blocking.getUnblocked().getSince()));
        } else if (blocking.isSetBlocked()) {
            shop.setBlockedReason(blocking.getBlocked().getReason());
            shop.setBlockedSince(TypeUtil.stringToLocalDateTime(blocking.getBlocked().getSince()));
        }

        final Shop mergedShop = shopEventMerger.mergeShop(partyId, shopId, shop);
        shopDao.saveShop(mergedShop);
    }

    @Override
    public EventType getChangeType() {
        return EventType.SHOP_BLOCKING;
    }
}
