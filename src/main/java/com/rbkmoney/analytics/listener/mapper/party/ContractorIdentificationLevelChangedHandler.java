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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ContractorIdentificationLevelChangedHandler extends AbstractClaimChangeHandler<Party> {

    private final PartyService partyService;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> {
            return claimEffect.isSetContractorEffect() && claimEffect.getContractorEffect().getEffect().isSetIdentificationLevelChanged();
        });
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event, LocalStorage<Party> storage) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetContractorEffect() && claimEffect.getContractorEffect().getEffect().isSetIdentificationLevelChanged()) {
                handleEvent(event, claimEffect, storage);
            }
        }
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect, LocalStorage<Party> storage) {
        ContractorEffectUnit contractorEffect = effect.getContractorEffect();
        ContractorIdentificationLevel identificationLevelChanged = contractorEffect.getEffect().getIdentificationLevelChanged();
        String contractorId = contractorEffect.getId();
        String partyId = event.getSourceId();

        Party party = partyService.getParty(partyId, storage);
        party.setContractorId(contractorId);
        party.setContractorIdentificationLevel(ContractorIdentificationLvl.valueOf(identificationLevelChanged.name()));

        partyService.saveParty(party);
        storage.put(partyId, party);
    }

}
