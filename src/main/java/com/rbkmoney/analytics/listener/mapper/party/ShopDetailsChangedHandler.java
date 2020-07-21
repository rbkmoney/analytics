package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.damsel.domain.ShopDetails;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.ShopEffectUnit;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class ShopDetailsChangedHandler extends AbstractClaimChangeHandler<List<Shop>> {

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetShopEffect()
                && claimEffect.getShopEffect().getEffect().isSetDetailsChanged());
    }

    @Override
    public List<Shop> handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        List<Shop> shopList = new ArrayList<>();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetDetailsChanged()) {
                shopList.add(handleEvent(event, claimEffect));
            }
        }

        return shopList;
    }

    private Shop handleEvent(MachineEvent event, ClaimEffect effect) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        ShopDetails detailsChanged = shopEffect.getEffect().getDetailsChanged();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();

        Shop shop = new Shop();
        shop.setPartyId(partyId);
        shop.setShopId(shopId);
        shop.setEventId(event.getEventId());
        shop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        shop.setDetailsName(detailsChanged.getName());
        shop.setDetailsDescription(detailsChanged.getDescription());

        return shop;
    }

}
