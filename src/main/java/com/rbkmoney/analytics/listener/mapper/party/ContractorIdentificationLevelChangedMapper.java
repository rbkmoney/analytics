package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.domain.db.enums.ContractorIdentificationLvl;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.domain.ContractorIdentificationLevel;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractorEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ContractorIdentificationLevelChangedMapper extends AbstractClaimChangeMapper<Party> {

    private final PartyService partyService;

    @Override
    public boolean accept(PartyChange change) {
        boolean accept = super.accept(change);
        if (accept) {
            List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
            return claimEffects.stream()
                    .anyMatch(claimEffect -> claimEffect.isSetContractorEffect()
                            && claimEffect.getContractorEffect().getEffect().isSetIdentificationLevelChanged());
        }
        return false;
    }

    @Override
    public Party map(PartyChange change, MachineEvent event, LocalStorage<Party> storage) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        ClaimEffect contractorEffect = claimEffects.stream()
                .filter(claimEffect -> claimEffect.isSetContractorEffect() && claimEffect.getContractorEffect().getEffect().isSetIdentificationLevelChanged())
                .findFirst().orElse(null);
        if (contractorEffect != null) {
            return mapEvent(event, contractorEffect, storage);
        }
        return null;
    }

    private Party mapEvent(MachineEvent event, ClaimEffect effect, LocalStorage<Party> storage) {
        ContractorEffectUnit contractorEffect = effect.getContractorEffect();
        ContractorIdentificationLevel identificationLevelChanged = contractorEffect.getEffect().getIdentificationLevelChanged();
        String contractorId = contractorEffect.getId();
        String partyId = event.getSourceId();

        Party party = partyService.getParty(partyId, storage);
        party.setContractorId(contractorId);
        party.setContractorIdentificationLevel(ContractorIdentificationLvl.valueOf(identificationLevelChanged.name()));

        return party;
    }

}
