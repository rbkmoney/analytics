package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.converter.ContractorToPartyConverter;
import com.rbkmoney.analytics.domain.db.enums.ContractorIdentificationLvl;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
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
public class ContractorCreatedHandler extends AbstractClaimChangeHandler<List<Party>> {

    private final ContractorToPartyConverter contractorToPartyConverter;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractorEffect()
                && claimEffect.getContractorEffect().getEffect().isSetCreated());
    }

    @Override
    public List<Party> handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        List<Party> partyList = new ArrayList<>();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetContractorEffect() && claimEffect.getContractorEffect().getEffect().isSetCreated()) {
                partyList.add(handleEvent(event, claimEffect));
            }
        }
        return partyList;
    }

    private Party handleEvent(MachineEvent event, ClaimEffect effect) {
        ContractorEffectUnit contractorEffect = effect.getContractorEffect();
        PartyContractor partyContractor = contractorEffect.getEffect().getCreated();
        Contractor contractor = partyContractor.getContractor();

        log.debug("ContractorCreatedHandler contractor: {}", contractor);

        String contractorId = contractorEffect.getId();
        String partyId = event.getSourceId();

        Party party = contractorToPartyConverter.convert(contractor);
        party.setPartyId(partyId);
        party.setEventId(event.getEventId());
        party.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        party.setContractorId(contractorId);
        party.setContractorIdentificationLevel(ContractorIdentificationLvl.valueOf(partyContractor.getStatus().name()));

        log.debug("ContractorCreatedHandler result party: {}", party);

        return party;
    }
}
