package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.ScheduleChanged;
import com.rbkmoney.damsel.payment_processing.ShopEffectUnit;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ShopPayoutScheduleChangedHandler extends AbstractClaimChangeHandler<Shop> {

    private final PartyService partyService;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> {
            return claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetPayoutScheduleChanged();
        });
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event, LocalStorage<Shop> storage) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetPayoutScheduleChanged()) {
                handleEvent(event, claimEffect, storage);
            }
        }
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect, LocalStorage<Shop> storage) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        ScheduleChanged payoutScheduleChanged = shopEffect.getEffect().getPayoutScheduleChanged();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();

        Shop shop = partyService.getShop(partyId, shopId, storage);
        if (payoutScheduleChanged.isSetSchedule()) {
            shop.setPayoutScheduleId(payoutScheduleChanged.getSchedule().getId());
        }

        partyService.saveShop(shop);
        storage.put(partyId + shopId, shop);
    }

}
