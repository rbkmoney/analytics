package com.rbkmoney.analytics.listener.mapper.party.contract;

import com.rbkmoney.analytics.domain.db.tables.pojos.ContractRef;
import com.rbkmoney.analytics.listener.mapper.party.AbstractClaimChangeHandler;
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
public class ContractCreatedHandler extends AbstractClaimChangeHandler<List<ContractRef>> {

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractEffect()
                && claimEffect.getContractEffect().getEffect().isSetCreated()
                && claimEffect.getContractEffect().getEffect().getCreated().isSetContractor());
    }

    @Override
    public List<ContractRef> handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        List<ContractRef> shops = new ArrayList<>();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetContractEffect()
                    && claimEffect.getContractEffect().getEffect().isSetCreated()
                    && claimEffect.getContractEffect().getEffect().getCreated().isSetContractor()) {
                shops.add(handleEvent(event, claimEffect));
            }
        }
        return shops;
    }

    private ContractRef handleEvent(MachineEvent event, ClaimEffect effect) {
        String partyId = event.getSourceId();
        ContractEffectUnit contractEffectUnit = effect.getContractEffect();
        com.rbkmoney.damsel.domain.Contract contractCreated = contractEffectUnit.getEffect().getCreated();
        Contractor contractor = contractCreated.getContractor();

        log.debug("ContractCreatedHandler contractor: {}", contractor);

        ContractRef contractRef = new ContractRef();
        contractRef.setPartyId(partyId);
        contractRef.setEventId(event.getEventId());
        contractRef.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        String contractorId = initContractorId(contractCreated);
        contractRef.setContractorId(contractorId);
        contractRef.setContractId( contractEffectUnit.getContractId());

        log.debug("ContractCreatedHandler result contract: {}", contractRef);

        return contractRef;
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
