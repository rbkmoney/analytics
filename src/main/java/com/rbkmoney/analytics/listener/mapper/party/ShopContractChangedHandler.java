package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.handler.party.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.analytics.service.model.ShopKey;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.ShopContractChanged;
import com.rbkmoney.damsel.payment_processing.ShopEffectUnit;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ShopContractChangedHandler extends AbstractClaimChangeHandler {

    private final PartyService partyService;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetShopEffect()
                && claimEffect.getShopEffect().getEffect().isSetContractChanged());

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event, LocalStorage localStorage) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetContractChanged()) {
                handleEvent(event, claimEffect, localStorage);
            }
        }
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect, LocalStorage localStorage) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        ShopContractChanged contractChanged = shopEffect.getEffect().getContractChanged();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();

        ShopKey shopKey = new ShopKey(partyId, shopId);
        Shop shop = partyService.getShop(shopKey, localStorage);
        shop.setEventId(event.getEventId());
        shop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        shop.setContractId(contractChanged.getContractId());
        shop.setPayoutToolId(contractChanged.getPayoutToolId());

        localStorage.putShop(shopKey, shop);
    }

}
