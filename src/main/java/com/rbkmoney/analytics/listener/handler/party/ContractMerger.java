package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.ContractRef;
import com.rbkmoney.analytics.service.PartyManagementService;
import com.rbkmoney.analytics.service.model.GeneralKey;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ContractMerger {

    private final PartyManagementService partyManagementService;

    public ContractRef merge(GeneralKey key, List<ContractRef> contractRefs) {
        ContractRef targetContract = partyManagementService.getContract(key.getRefId());
        if (targetContract == null) {
            targetContract = new ContractRef();
        }
        for (ContractRef contractRef : contractRefs) {
            targetContract.setEventId(contractRef.getEventId());
            targetContract.setEventTime(contractRef.getEventTime());
            targetContract.setPartyId(key.getPartyId());
            targetContract.setContractId(contractRef.getContractId() != null ? contractRef.getContractId() : targetContract.getContractId());
            targetContract.setContractorId(contractRef.getContractorId() != null ? contractRef.getContractorId() : targetContract.getContractorId());
        }
        return targetContract;
    }
}
