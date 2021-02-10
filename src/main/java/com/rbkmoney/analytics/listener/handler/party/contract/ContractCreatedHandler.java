package com.rbkmoney.analytics.listener.handler.party.contract;

import com.rbkmoney.analytics.converter.ContractorToCurrentContractorConverter;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractorDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.analytics.listener.handler.party.AbstractClaimChangeHandler;
import com.rbkmoney.damsel.domain.Contractor;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Component
@Order(3)
@RequiredArgsConstructor
public class ContractCreatedHandler extends AbstractClaimChangeHandler {

    private final ContractorToCurrentContractorConverter contractorToCurrentContractorConverter;
    private final ContractorDao contractorDao;
    private final ContractDao contractDao;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractEffect()
                && claimEffect.getContractEffect().getEffect().isSetCreated());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        log.debug("ContractCreatedHandler handleChange change: {}", change);
        getClaimStatus(change).getAccepted().getEffects().stream()
                .filter(claimEffect -> claimEffect.isSetContractEffect()
                        && claimEffect.getContractEffect().getEffect().isSetCreated())
                .forEach(claimEffect -> handleEvent(event, claimEffect));
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        String partyId = event.getSourceId();
        ContractEffectUnit contractEffectUnit = effect.getContractEffect();
        com.rbkmoney.damsel.domain.Contract contractCreated = contractEffectUnit.getEffect().getCreated();
        Contractor contractor = contractCreated.getContractor();

        log.debug("ContractCreatedHandler contractor: {}", contractor);

        Contract contract = new Contract();
        contract.setPartyId(partyId);
        contract.setEventId(event.getEventId());
        contract.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));

        String contractorId = initContractorId(contractCreated);
        com.rbkmoney.analytics.domain.db.tables.pojos.Contractor currentContractor = null;

        if (contractor != null) {
            currentContractor = contractorToCurrentContractorConverter.convert(contractor);
            currentContractor.setContractorId(contractorId);
            currentContractor.setEventId(event.getEventId());
            currentContractor.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            currentContractor.setPartyId(partyId);
            contractorDao.saveContractor(currentContractor);
        } else {
            currentContractor = contractorDao.getContractorByPartyIdAndContractorId(partyId, contractorId);
        }

        contract.setContractorId(contractorId);
        contract.setContractId(contractEffectUnit.getContractId());
        contractDao.saveContract(contract);

        log.debug("ContractCreatedHandler result contract: {} currentContractor: {}", contract, currentContractor);
    }

    private String initContractorId(com.rbkmoney.damsel.domain.Contract contractCreated) {
        String contractorId = "";
        if (contractCreated.isSetContractorId()) {
            contractorId = contractCreated.getContractorId();
        } else if (contractCreated.isSetContractor()) {
            contractorId = UUID.randomUUID().toString();
        }
        return contractorId;
    }

}
