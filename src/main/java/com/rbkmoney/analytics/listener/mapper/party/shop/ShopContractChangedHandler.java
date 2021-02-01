package com.rbkmoney.analytics.listener.mapper.party.shop;

import com.rbkmoney.analytics.converter.ContractorToShopConverter;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contractor;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.party.AbstractClaimChangeHandler;
import com.rbkmoney.analytics.service.PartyManagementService;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.ShopContractChanged;
import com.rbkmoney.damsel.payment_processing.ShopEffectUnit;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class ShopContractChangedHandler extends AbstractClaimChangeHandler<List<Shop>> {

    private final PartyManagementService partyManagementService;
    private final ContractorToShopConverter contractorToShopConverter;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetShopEffect()
                && claimEffect.getShopEffect().getEffect().isSetContractChanged());

    }

    @Override
    public List<Shop> handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        List<Shop> shopList = new ArrayList<>();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetShopEffect() && claimEffect.getShopEffect().getEffect().isSetContractChanged()) {
                shopList.add(handleEvent(event, claimEffect));
            }
        }

        return shopList;
    }

    private Shop handleEvent(MachineEvent event, ClaimEffect effect) {
        ShopEffectUnit shopEffect = effect.getShopEffect();
        ShopContractChanged contractChanged = shopEffect.getEffect().getContractChanged();
        String shopId = shopEffect.getShopId();
        String partyId = event.getSourceId();
        final String contractId = contractChanged.getContractId();
        final Contract contract = partyManagementService.getContract(contractId);
        final Contractor contractor = partyManagementService.getContractorById(contract.getContractorId());

        Shop shop = contractorToShopConverter.convert(contractor);
        shop.setPartyId(partyId);
        shop.setShopId(shopId);
        shop.setEventId(event.getEventId());
        shop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        shop.setContractId(contractChanged.getContractId());
        shop.setPayoutToolId(contractChanged.getPayoutToolId());

        return shop;
    }

}
