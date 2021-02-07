package com.rbkmoney.analytics.listener.handler.party.contract;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.analytics.listener.handler.merger.ContractMerger;
import com.rbkmoney.analytics.listener.handler.party.AbstractClaimChangeHandler;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class ContractContractorIDChangedHandler extends AbstractClaimChangeHandler {

    private final ContractDao contractDao;
    private final ContractMerger contractMerger;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractEffect()
                && claimEffect.getContractEffect().getEffect().isSetContractorChanged());
    }

    @Override
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

        log.debug("ContractorChangeIdHandler save contract: {}", mergedContract);
    }

}
