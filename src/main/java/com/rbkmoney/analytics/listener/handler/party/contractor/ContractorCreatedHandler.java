package com.rbkmoney.analytics.listener.handler.party.contractor;

import com.rbkmoney.analytics.converter.ContractorToCurrentContractorConverter;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractorDao;
import com.rbkmoney.analytics.domain.db.enums.ContractorIdentificationLvl;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contractor;
import com.rbkmoney.analytics.listener.handler.merger.ContractorEventMerger;
import com.rbkmoney.analytics.listener.handler.party.AbstractClaimChangeHandler;
import com.rbkmoney.damsel.domain.PartyContractor;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractorEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Order(3)
@Component
@RequiredArgsConstructor
public class ContractorCreatedHandler extends AbstractClaimChangeHandler {

    private final ContractorToCurrentContractorConverter contractorToCurrentContractorConverter;
    private final ContractorDao contractorDao;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractorEffect()
                && claimEffect.getContractorEffect().getEffect().isSetCreated());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        getClaimStatus(change).getAccepted().getEffects().stream()
                .filter(claimEffect -> claimEffect.isSetContractorEffect() && claimEffect.getContractorEffect().getEffect().isSetCreated())
                .forEach(claimEffect -> handleEvent(event, claimEffect));
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        ContractorEffectUnit contractorEffect = effect.getContractorEffect();
        PartyContractor partyContractor = contractorEffect.getEffect().getCreated();
        com.rbkmoney.damsel.domain.Contractor contractor = partyContractor.getContractor();

        log.debug("ContractorCreatedHandler contractor: {}", contractor);

        String contractorId = contractorEffect.getId();
        String partyId = event.getSourceId();

        Contractor currentContractor = contractorToCurrentContractorConverter.convert(contractor);
        currentContractor.setPartyId(partyId);
        currentContractor.setEventId(event.getEventId());
        currentContractor.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        currentContractor.setContractorId(contractorId);
        currentContractor.setContractorIdentificationLevel(ContractorIdentificationLvl.valueOf(partyContractor.getStatus().name()));

        log.debug("ContractorCreatedHandler result contractor: {}", currentContractor);

        contractorDao.saveContractor(currentContractor);
    }
}
