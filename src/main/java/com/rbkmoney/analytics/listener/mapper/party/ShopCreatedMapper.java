package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.BlockingType;
import com.rbkmoney.analytics.constant.SuspensionType;
import com.rbkmoney.analytics.dao.model.ShopRow;
import com.rbkmoney.damsel.domain.Shop;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.ShopEffectUnit;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;

import java.time.LocalDateTime;
import java.util.List;

public class ShopCreatedMapper extends AbstractClaimChangeMapper<ShopRow> {

    @Override
    public ShopRow map(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        ClaimEffect contractorEffect = claimEffects.stream()
                .filter(claimEffect -> claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetCreated())
                .findFirst().orElse(null);
        if (contractorEffect != null) {
            return mapEvent(event, contractorEffect);
        }
        return null;
    }

    private ShopRow mapEvent(MachineEvent event, ClaimEffect effect) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        Shop shopCreated = shopEffect.getEffect().getCreated();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();
        LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());

        ShopRow shopRow = new ShopRow();
        shopRow.setEventTime(eventCreatedAt);
        shopRow.setShopdId(shopId);
        shopRow.setPartyId(partyId);
        shopRow.setCreatedAt(TypeUtil.stringToLocalDateTime(shopCreated.getCreatedAt()));
        shopRow.setBlocking(TBaseUtil.unionFieldToEnum(shopCreated.getBlocking(), BlockingType.class));
        if (shopCreated.getBlocking().isSetUnblocked()) {
            shopRow.setUnblockedReason(shopCreated.getBlocking().getUnblocked().getReason());
            shopRow.setUnblockedSince(TypeUtil.stringToLocalDateTime(shopCreated.getBlocking().getUnblocked().getSince()));
        } else if (shopCreated.getBlocking().isSetBlocked()) {
            shopRow.setBlockedReason(shopCreated.getBlocking().getBlocked().getReason());
            shopRow.setBlockedSince(TypeUtil.stringToLocalDateTime(shopCreated.getBlocking().getBlocked().getSince()));
        }
        shopRow.setSuspension(TBaseUtil.unionFieldToEnum(shopCreated.getSuspension(), SuspensionType.class));
       if (shopCreated.getSuspension().isSetActive()) {
           shopRow.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(shopCreated.getSuspension().getActive().getSince()));
       } else if (shopCreated.getSuspension().isSetSuspended()) {
           shopRow.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(shopCreated.getSuspension().getSuspended().getSince()));
       }
       shopRow.setDetailsName(shopCreated.getDetails().getName());
       shopRow.setDetailsDescription(shopCreated.getDetails().getDescription());
       if (shopCreated.getLocation().isSetUrl()) {
           shopRow.setLocationUrl(shopCreated.getLocation().getUrl());
       }
       shopRow.setCategoryId(String.valueOf(shopCreated.getCategory().getId()));
       if (shopCreated.isSetAccount()) {
           shopRow.setAccountCurrencyCode(shopCreated.getAccount().getCurrency().getSymbolicCode());
           shopRow.setAccountGuarantee(String.valueOf(shopCreated.getAccount().getGuarantee()));
           shopRow.setAccountPayout(String.valueOf(shopCreated.getAccount().getPayout()));
           shopRow.setAccountSettlement(String.valueOf(shopCreated.getAccount().getSettlement()));
       }
       shopRow.setContractId(shopCreated.getContractId());
       shopRow.setPayoutToolId(shopCreated.getPayoutToolId());
       if (shopCreated.isSetPayoutSchedule()) {
           shopRow.setPayoutScheduleId(String.valueOf(shopCreated.getPayoutSchedule().getId()));
       }

       return shopRow;
    }


}
