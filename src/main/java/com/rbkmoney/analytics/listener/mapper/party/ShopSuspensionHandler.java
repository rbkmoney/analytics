package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.domain.Suspension;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class ShopSuspensionHandler implements ChangeHandler<PartyChange, MachineEvent, Shop> {

    private final PartyService partyService;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        Suspension suspension = change.getShopSuspension().getSuspension();
        String shopId = change.getShopSuspension().getShopId();
        String partyId = event.getSourceId();

        Shop shop = partyService.getShop(partyId, shopId);
        if (suspension.isSetActive()) {
            shop.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(suspension.getActive().getSince()));
        } else if (suspension.isSetSuspended()) {
            shop.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(suspension.getSuspended().getSince()));
        }

        partyService.saveShop(shop);
    }

    @Override
    public EventType getChangeType() {
        return EventType.SHOP_SUSPENSION;
    }

}
