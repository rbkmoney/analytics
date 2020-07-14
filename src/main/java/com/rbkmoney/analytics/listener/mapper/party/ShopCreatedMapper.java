package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.domain.db.enums.Blocking;
import com.rbkmoney.analytics.domain.db.enums.Suspension;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.ShopEffectUnit;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ShopCreatedMapper extends AbstractClaimChangeMapper<Shop> {

    @Override
    public boolean accept(PartyChange change) {
        boolean accept = super.accept(change);
        if (accept) {
            List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
            return claimEffects.stream()
                    .anyMatch(claimEffect -> claimEffect.isSetShopEffect()
                            && claimEffect.getShopEffect().getEffect().isSetCreated());
        }
        return false;
    }

    @Override
    public Shop map(PartyChange change, MachineEvent event, LocalStorage<Shop> storage) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        ClaimEffect contractorEffect = claimEffects.stream()
                .filter(claimEffect -> claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetCreated())
                .findFirst().orElse(null);
        if (contractorEffect != null) {
            return mapEvent(event, contractorEffect, storage);
        }
        return null;
    }

    private Shop mapEvent(MachineEvent event, ClaimEffect effect, LocalStorage<Shop> storage) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        com.rbkmoney.damsel.domain.Shop shopCreated = shopEffect.getEffect().getCreated();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();

        Shop shop = new Shop();
        shop.setShopId(shopId);
        shop.setPartyId(partyId);
        shop.setCreatedAt(TypeUtil.stringToLocalDateTime(shopCreated.getCreatedAt()));
        shop.setBlocking(TBaseUtil.unionFieldToEnum(shopCreated.getBlocking(), Blocking.class));
        if (shopCreated.getBlocking().isSetUnblocked()) {
            shop.setUnblockedReason(shopCreated.getBlocking().getUnblocked().getReason());
            shop.setUnblockedSince(TypeUtil.stringToLocalDateTime(shopCreated.getBlocking().getUnblocked().getSince()));
        } else if (shopCreated.getBlocking().isSetBlocked()) {
            shop.setBlockedReason(shopCreated.getBlocking().getBlocked().getReason());
            shop.setBlockedSince(TypeUtil.stringToLocalDateTime(shopCreated.getBlocking().getBlocked().getSince()));
        }
        shop.setSuspension(TBaseUtil.unionFieldToEnum(shopCreated.getSuspension(), Suspension.class));
       if (shopCreated.getSuspension().isSetActive()) {
           shop.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(shopCreated.getSuspension().getActive().getSince()));
       } else if (shopCreated.getSuspension().isSetSuspended()) {
           shop.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(shopCreated.getSuspension().getSuspended().getSince()));
       }
       shop.setDetailsName(shopCreated.getDetails().getName());
       shop.setDetailsDescription(shopCreated.getDetails().getDescription());
       if (shopCreated.getLocation().isSetUrl()) {
           shop.setLocationUrl(shopCreated.getLocation().getUrl());
       }
       shop.setCategoryId(shopCreated.getCategory().getId());
       if (shopCreated.isSetAccount()) {
           shop.setAccountCurrencyCode(shopCreated.getAccount().getCurrency().getSymbolicCode());
           shop.setAccountGuarantee(String.valueOf(shopCreated.getAccount().getGuarantee()));
           shop.setAccountPayout(String.valueOf(shopCreated.getAccount().getPayout()));
           shop.setAccountSettlement(String.valueOf(shopCreated.getAccount().getSettlement()));
       }
       shop.setContractId(shopCreated.getContractId());
       shop.setPayoutToolId(shopCreated.getPayoutToolId());
       if (shopCreated.isSetPayoutSchedule()) {
           shop.setPayoutScheduleId(shopCreated.getPayoutSchedule().getId());
       }

       return shop;
    }


}
