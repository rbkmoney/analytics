package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.converter.ContractorToPartyConverter;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.damsel.domain.Contractor;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class ContractCreatedHandler extends AbstractClaimChangeHandler<List<Party>> {

    private final ContractorToPartyConverter contractorToPartyConverter;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractEffect()
                && claimEffect.getContractEffect().getEffect().isSetCreated());
    }

    @Override
    public List<Party> handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        List<Party> partyList = new ArrayList<>();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetContractEffect()
                    && claimEffect.getContractEffect().getEffect().isSetCreated()
                    && claimEffect.getContractEffect().getEffect().getCreated().isSetContractor()) {
                partyList.add(handleEvent(event, claimEffect));
            }
        }
        return partyList;
    }

    private Party handleEvent(MachineEvent event, ClaimEffect effect) {
        String partyId = event.getSourceId();
        ContractEffectUnit contractEffectUnit = effect.getContractEffect();
        com.rbkmoney.damsel.domain.Contract contractCreated = contractEffectUnit.getEffect().getCreated();
        Contractor contractor = contractCreated.getContractor();

        log.debug("ContractCreatedHandler contractor: {}", contractor);

        Party party = contractorToPartyConverter.convert(contractor);
        party.setPartyId(partyId);
        party.setEventId(event.getEventId());
        party.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        String contractorId = initContractorId(contractCreated);
        party.setContractorId(contractorId);

        log.debug("ContractCreatedHandler result party: {}", party);

        return party;
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
