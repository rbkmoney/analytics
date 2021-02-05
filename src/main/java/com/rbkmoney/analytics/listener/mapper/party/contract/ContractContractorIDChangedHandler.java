package com.rbkmoney.analytics.listener.mapper.party.contract;

import com.rbkmoney.analytics.converter.ContractorToCurrentContractorConverter;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.analytics.listener.mapper.party.AbstractClaimChangeHandler;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class ContractContractorIDChangedHandler extends AbstractClaimChangeHandler<List<Contract>> {

    private final ContractDao contractDao;

    @Override
    public boolean accept(PartyChange change) {
        return isClaimEffect(change, claimEffect -> claimEffect.isSetContractEffect()
                && claimEffect.getContractEffect().getEffect().isSetContractorChanged());
    }

    @Override
    public List<Contract> handleChange(PartyChange change, MachineEvent event) {
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects();
        List<Contract> contracts = new ArrayList<>();
        for (ClaimEffect claimEffect : claimEffects) {
            if (claimEffect.isSetContractEffect() && claimEffect.getContractEffect().getEffect().isSetContractorChanged()) {
                contracts.add(handleEvent(event, claimEffect));
            }
        }
        return contracts;
    }

    private Contract handleEvent(MachineEvent event, ClaimEffect effect) {
        log.debug("ContractorChangeIdHandler contractor: {}", event);
        ContractEffectUnit contractEffectUnit = effect.getContractEffect();
        final Contract contract = contractDao.getContractById(contractEffectUnit.getContractId());
        contract.setContractorId(contractEffectUnit.getEffect().getContractorChanged());
        log.debug("ContractorChangeIdHandler result contract: {}", contract);
        return contract;
    }

}
