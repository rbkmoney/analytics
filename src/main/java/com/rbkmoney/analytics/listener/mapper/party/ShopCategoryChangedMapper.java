package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.dao.model.ShopRow;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.ShopEffectUnit;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;

import java.time.LocalDateTime;
import java.util.List;

public class ShopCategoryChangedMapper extends AbstractClaimChangeMapper<ShopRow> {

    @Override
    public ShopRow map(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        ClaimEffect contractorEffect = claimEffects.stream()
                .filter(claimEffect -> claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetCategoryChanged())
                .findFirst().orElse(null);
        if (contractorEffect != null) {
            return mapEvent(event, contractorEffect);
        }
        return null;
    }

    private ShopRow mapEvent(MachineEvent event, ClaimEffect effect) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        int categoryId = shopEffect.getEffect().getCategoryChanged().getId();
        LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();

        ShopRow shopRow = new ShopRow();
        shopRow.setEventTime(eventCreatedAt);
        shopRow.setShopdId(shopId);
        shopRow.setPartyId(partyId);
        shopRow.setCategoryId(String.valueOf(categoryId));

        return shopRow;
    }


}
