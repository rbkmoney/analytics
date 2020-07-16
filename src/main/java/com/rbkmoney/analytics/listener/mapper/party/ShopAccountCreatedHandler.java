package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.domain.ShopAccount;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.ShopEffectUnit;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ShopAccountCreatedHandler extends AbstractClaimChangeHandler<Shop> {

    private final PartyService partyService;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> {
            return claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetAccountCreated();
        });

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event, LocalStorage<Shop> storage) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetAccountCreated()) {
                handleEvent(event, claimEffect, storage);
            }
        }
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect, LocalStorage<Shop> storage) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        ShopAccount accountCreated = shopEffect.getEffect().getAccountCreated();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();

        Shop shop = partyService.getShop(partyId, shopId, storage);
        shop.setAccountCurrencyCode(accountCreated.getCurrency().getSymbolicCode());
        shop.setAccountGuarantee(String.valueOf(accountCreated.getGuarantee()));
        shop.setAccountSettlement(String.valueOf(accountCreated.getSettlement()));
        shop.setAccountPayout(String.valueOf(accountCreated.getPayout()));

        partyService.saveShop(shop);
        storage.put(partyId + shopId, shop);
    }

}
