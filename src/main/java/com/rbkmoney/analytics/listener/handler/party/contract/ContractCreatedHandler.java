package com.rbkmoney.analytics.listener.handler.party.contract;

import com.rbkmoney.analytics.converter.ContractorToCurrentContractorConverter;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractorDao;
import com.rbkmoney.analytics.domain.db.enums.ContractStatus;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.analytics.listener.handler.party.AbstractClaimChangeHandler;
import com.rbkmoney.damsel.domain.Contractor;
import com.rbkmoney.damsel.payment_processing.ClaimEffect;
import com.rbkmoney.damsel.payment_processing.ContractEffectUnit;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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
        log.debug("Contract create change: {}", change);
        List<ClaimEffect> claimEffects = getClaimStatus(change).getAccepted().getEffects().stream()
                .filter(claimEffect -> claimEffect.isSetContractEffect()
                        && claimEffect.getContractEffect().getEffect().isSetCreated())
                .collect(Collectors.toList());
        for (ClaimEffect claimEffect : claimEffects) {
            handleEvent(event, claimEffect);
        }
        log.debug("Contract create finished");
    }

    private void handleEvent(MachineEvent event, ClaimEffect effect) {
        log.debug("Contract create handle: {}", effect);

        String partyId = event.getSourceId();
        ContractEffectUnit contractEffectUnit = effect.getContractEffect();
        com.rbkmoney.damsel.domain.Contract contractCreated = contractEffectUnit.getEffect().getCreated();

        Contract contract = new Contract();
        contract.setPartyId(partyId);
        contract.setEventId(event.getEventId());
        contract.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        contract.setCreatedAt(TypeUtil.stringToLocalDateTime(contractCreated.getCreatedAt()));
        contract.setValidSince(TypeUtil.stringToLocalDateTime(contractCreated.getValidSince()));
        contract.setValidUntil(TypeUtil.stringToLocalDateTime(contractCreated.getValidUntil()));
        contract.setStatus(TBaseUtil.unionFieldToEnum(contractCreated.getStatus(), ContractStatus.class));
        contract.setStatusTerminatedAt(
                TypeUtil.stringToLocalDateTime(contractCreated.getStatus().getTerminated().getTerminatedAt())
        );
        contract.setLegalAgreementId(contract.getLegalAgreementId());
        contract.setLegalAgreementSignedAt(contract.getLegalAgreementSignedAt());
        contract.setLegalAgreementValidUntil(contract.getLegalAgreementValidUntil());

        String contractorId = checkAndCreateContractor(event, partyId, contractCreated);
        contract.setContractorId(contractorId);
        contract.setContractId(contractEffectUnit.getContractId());
        contractDao.saveContract(contract);

        log.debug("Contract create result: {}", contract);
    }

    private String checkAndCreateContractor(MachineEvent event, String partyId,
                                            com.rbkmoney.damsel.domain.Contract contractCreated) {
        Contractor contractor = contractCreated.getContractor();
        log.debug("ContractCreatedHandler contractor: {}", contractor);
        String contractorId = initContractorId(contractCreated);
        if (contractor != null) {
            com.rbkmoney.analytics.domain.db.tables.pojos.Contractor currentContractor =
                    contractorToCurrentContractorConverter.convert(contractor);
            currentContractor.setContractorId(contractorId);
            currentContractor.setEventId(event.getEventId());
            currentContractor.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            currentContractor.setPartyId(partyId);
            contractorDao.saveContractor(currentContractor);
            log.debug("ContractCreatedHandler save currentContractor: {}", currentContractor);
        }
        return contractorId;
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
