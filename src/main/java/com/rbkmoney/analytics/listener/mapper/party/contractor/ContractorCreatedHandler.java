package com.rbkmoney.analytics.listener.mapper.party.contractor;

import com.rbkmoney.analytics.converter.ContractorToCurrentContractorConverter;
import com.rbkmoney.analytics.domain.db.enums.ContractorIdentificationLvl;
import com.rbkmoney.analytics.domain.db.tables.pojos.CurrentContractor;
import com.rbkmoney.analytics.listener.mapper.party.AbstractClaimChangeHandler;
import com.rbkmoney.damsel.domain.Contractor;
import com.rbkmoney.damsel.domain.PartyContractor;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractorEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ContractorCreatedHandler extends AbstractClaimChangeHandler<List<CurrentContractor>> {

    private final ContractorToCurrentContractorConverter contractorToShopConverter;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractorEffect()
                && claimEffect.getContractorEffect().getEffect().isSetCreated());
    }

    @Override
    public List<CurrentContractor> handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        List<CurrentContractor> currentContractors = new ArrayList<>();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetContractorEffect() && claimEffect.getContractorEffect().getEffect().isSetCreated()) {
                currentContractors.add(handleEvent(event, claimEffect));
            }
        }
        return currentContractors;
    }

    private CurrentContractor handleEvent(MachineEvent event, ClaimEffect effect) {
        ContractorEffectUnit contractorEffect = effect.getContractorEffect();
        PartyContractor partyContractor = contractorEffect.getEffect().getCreated();
        Contractor contractor = partyContractor.getContractor();

        log.debug("ContractorCreatedHandler contractor: {}", contractor);

        String contractorId = contractorEffect.getId();
        String partyId = event.getSourceId();

        CurrentContractor currentContractor = contractorToShopConverter.convert(contractor);
        currentContractor.setPartyId(partyId);
        currentContractor.setEventId(event.getEventId());
        currentContractor.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        currentContractor.setContractorId(contractorId);
        currentContractor.setContractorIdentificationLevel(ContractorIdentificationLvl.valueOf(partyContractor.getStatus().name()));

        log.debug("ContractorCreatedHandler result contractor: {}", currentContractor);

        return currentContractor;
    }
}
