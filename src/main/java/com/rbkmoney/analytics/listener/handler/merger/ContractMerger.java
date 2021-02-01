package com.rbkmoney.analytics.listener.handler.merger;

import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.analytics.service.PartyManagementService;
import com.rbkmoney.analytics.service.model.PartyGeneralKey;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ContractMerger {

    private final PartyManagementService partyManagementService;

    public Contract merge(PartyGeneralKey key, List<Contract> contractRefs) {
        Contract targetContract = partyManagementService.getContract(key.getRefId());
        if (targetContract == null) {
            targetContract = new Contract();
        }
        for (Contract contract : contractRefs) {
            targetContract.setEventId(contract.getEventId());
            targetContract.setEventTime(contract.getEventTime());
            targetContract.setPartyId(key.getPartyId());
            targetContract.setContractId(contract.getContractId() != null ? contract.getContractId() : targetContract.getContractId());
            targetContract.setContractorId(contract.getContractorId() != null ? contract.getContractorId() : targetContract.getContractorId());
        }
        return targetContract;
    }
}
