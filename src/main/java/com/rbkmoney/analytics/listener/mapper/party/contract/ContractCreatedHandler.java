package com.rbkmoney.analytics.listener.mapper.party.contract;

import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
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
public class ContractCreatedHandler extends AbstractClaimChangeHandler<List<Contract>> {

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractEffect()
                && claimEffect.getContractEffect().getEffect().isSetCreated()
                && claimEffect.getContractEffect().getEffect().getCreated().isSetContractor());
    }

    @Override
    public List<Contract> handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        List<Contract> contracts = new ArrayList<>();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetContractEffect()
                    && claimEffect.getContractEffect().getEffect().isSetCreated()
                    && claimEffect.getContractEffect().getEffect().getCreated().isSetContractor()) {
                contracts.add(handleEvent(event, claimEffect));
            }
        }
        return contracts;
    }

    private Contract handleEvent(MachineEvent event, ClaimEffect effect) {
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
        contract.setContractorId(contractorId);
        contract.setContractId(contractEffectUnit.getContractId());

        log.debug("ContractCreatedHandler result contract: {}", contract);

        return contract;
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
