package com.rbkmoney.analytics.listener.handler.merger;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ContractMerger {

    private final ContractDao contractDao;

    public Contract merge(String contractId, Contract contract) {
        Contract targetContract = contractDao.getContractByPartyIdAndContractId(contract.getPartyId(), contractId);
        if (targetContract == null) {
            targetContract = new Contract();
        }
        targetContract.setEventId(contract.getEventId());
        targetContract.setEventTime(contract.getEventTime());
        targetContract.setContractId(contract.getContractId() != null ? contract.getContractId() : targetContract.getContractId());
        targetContract.setContractorId(contract.getContractorId() != null ? contract.getContractorId() : targetContract.getContractorId());
        return targetContract;
    }
}
