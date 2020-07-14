package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.ShopContractChanged;
import com.rbkmoney.damsel.payment_processing.ShopEffectUnit;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ShopContractChangedMapper extends AbstractClaimChangeMapper<Shop> {

    private final PartyService partyService;

    @Override
    public boolean accept(PartyChange change) {
        boolean accept = super.accept(change);
        if (accept) {
            List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
            return claimEffects.stream()
                    .anyMatch(claimEffect -> claimEffect.isSetShopEffect()
                            && claimEffect.getShopEffect().getEffect().isSetContractChanged());
        }
        return false;
    }

    @Override
    public Shop map(PartyChange change, MachineEvent event, LocalStorage<Shop> storage) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        ClaimEffect contractorEffect = claimEffects.stream()
                .filter(claimEffect -> claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetContractChanged())
                .findFirst().orElse(null);
        if (contractorEffect != null) {
            return mapEvent(event, contractorEffect, storage);
        }
        return null;
    }

    private Shop mapEvent(MachineEvent event, ClaimEffect effect, LocalStorage<Shop> storage) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        ShopContractChanged contractChanged = shopEffect.getEffect().getContractChanged();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();

        Shop shop = partyService.getShop(partyId, shopId, storage);
        shop.setContractId(contractChanged.getContractId());
        shop.setPayoutToolId(contractChanged.getPayoutToolId());

        return shop;
    }

}
