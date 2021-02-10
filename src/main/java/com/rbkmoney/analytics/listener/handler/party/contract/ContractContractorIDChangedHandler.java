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
        getClaimStatus(change).getAccepted().getEffects().stream()
                .filter(claimEffect -> claimEffect.isSetContractEffect() && claimEffect.getContractEffect().getEffect().isSetContractorChanged())
                .forEach(claimEffect -> handleEvent(event, claimEffect));
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        log.debug("ContractorChangeIdHandler contractor: {}", event);
        ContractEffectUnit contractEffectUnit = effect.getContractEffect();
        String partyId = event.getSourceId();

        final Contract contract = new Contract();
        contract.setPartyId(partyId);
        contract.setContractorId(contractEffectUnit.getEffect().getContractorChanged());
        contract.setEventId(event.getEventId());
        contract.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));

        final Contract mergedContract = contractMerger.merge(contractEffectUnit.getContractId(), contract);
        contractDao.saveContract(mergedContract);
        final List<Shop> updatedShops = updateShops(event, partyId, contract, mergedContract);
        log.debug("ContractorChangeIdHandler save shops: {} and contractor: {}", updatedShops, mergedContract);
    }

    @Nullable
    private List<Shop> updateShops(MachineEvent event, String partyId, Contract contract, Contract mergedContract) {
        final List<Shop> currentShopStates = shopDao.getShopsByPartyIdAndContractId(partyId, mergedContract.getContractId());
        if (currentShopStates != null) {
            final Contractor currentContractorState = contractorDao.getContractorByPartyIdAndContractorId(partyId,
                    contract.getContractorId());
            currentShopStates.forEach(shop -> mergeAndUpdateShop(shop, event, partyId, currentContractorState));
        }
        return currentShopStates;
    }

    private void mergeAndUpdateShop(Shop currentShopState, MachineEvent event, String partyId, Contractor currentContractorState) {
        final Shop shop = contractorToShopConverter.convert(currentContractorState);
        final Shop mergedShop = shopEventMerger.mergeShop(partyId, currentShopState.getShopId(), shop);
        mergedShop.setEventId(event.getEventId());
        mergedShop.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        shopDao.saveShop(mergedShop);
    }

}
