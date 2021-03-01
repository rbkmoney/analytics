package com.rbkmoney.analytics.listener.handler.party.contract;

import com.rbkmoney.analytics.converter.ContractorToShopConverter;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractorDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ShopDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contractor;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.handler.merger.ContractMerger;
import com.rbkmoney.analytics.listener.handler.merger.ShopEventMerger;
import com.rbkmoney.analytics.listener.handler.party.AbstractClaimChangeHandler;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Component
public class ContractContractorIDChangedHandler extends AbstractClaimChangeHandler {

    private final ContractDao contractDao;
    private final ContractorDao contractorDao;
    private final ShopDao shopDao;
    private final ContractMerger contractMerger;
    private final ShopEventMerger shopEventMerger;
    private final ContractorToShopConverter contractorToShopConverter;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractEffect()
                && claimEffect.getContractEffect().getEffect().isSetContractorChanged());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects().stream()
                .filter(claimEffect -> claimEffect.isSetContractEffect()
                        && claimEffect.getContractEffect().getEffect().isSetContractorChanged())
                .collect(Collectors.toList());
        for (ClaimEffect claimEffect : claimEffects) {
            handleEvent(event, claimEffect);
        }
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        ContractEffectUnit contractEffectUnit = effect.getContractEffect();
        String partyId = event.getSourceId();

        final Contract contract = new Contract();
        contract.setPartyId(partyId);
        contract.setContractorId(contractEffectUnit.getEffect().getContractorChanged());
        contract.setEventId(event.getEventId());
        contract.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));

        final Contract mergedContract = contractMerger.merge(contractEffectUnit.getContractId(), contract);
        contractDao.saveContract(mergedContract);
        updateShops(event, partyId, contract, mergedContract);
    }

    @Nullable
    private List<Shop> updateShops(MachineEvent event, String partyId, Contract contract, Contract mergedContract) {
        log.debug("Update shops with partyId: {} and contract: {}", partyId, contract);
        List<Shop> currentShopStates = shopDao.getShopsByPartyIdAndContractId(partyId, mergedContract.getContractId());
        log.debug("Update shops: {}", currentShopStates);
        if (currentShopStates != null) {
            Contractor currentContractorState = contractorDao.getContractorByPartyIdAndContractorId(partyId,
                    contract.getContractorId());
            Shop newShop = contractorToShopConverter.convert(currentContractorState);
            List<Shop> batch = currentShopStates.stream()
                    .map(shop -> map(shop, event, partyId, newShop))
                    .collect(Collectors.toList());
            shopDao.saveShop(batch);
            log.debug("Shops saved: {}", currentShopStates);
        }
        return currentShopStates;
    }

    private Shop map(Shop currentShopState, MachineEvent event, String partyId, Shop newShop) {
        Shop mergedShop = shopEventMerger.mergeShop(partyId, currentShopState.getShopId(), newShop, currentShopState);
        mergedShop.setEventId(event.getEventId());
        mergedShop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        return mergedShop;
    }

}
