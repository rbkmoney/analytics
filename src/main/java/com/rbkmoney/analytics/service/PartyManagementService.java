package com.rbkmoney.analytics.service;

import com.rbkmoney.analytics.dao.repository.postgres.PostgresPartyDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.ContractRef;
import com.rbkmoney.analytics.domain.db.tables.pojos.CurrentContractor;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.service.model.GeneralKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PartyManagementService {

    private final PostgresPartyDao postgresPartyDao;

    public Party getParty(String partyId) {
        Party party = postgresPartyDao.getPartyForUpdate(partyId);
        log.debug("Get party from DB by partyId={}. Result={}", partyId, party);

        return party;
    }

    public ContractRef getContract(String contractId) {
        ContractRef contractForUpdate = postgresPartyDao.getContractForUpdate(contractId);
        log.debug("Get contract from DB by contractId={}. Result={}", contractId, contractForUpdate);
        return contractForUpdate;
    }

    public void saveParty(List<Party> partyList) {
        log.debug("Save parties: {}", partyList);
        postgresPartyDao.saveParty(partyList);
    }

    public void saveContractRefs(List<ContractRef> contractRefs) {
        log.debug("Save contract: {}", contractRefs);
        postgresPartyDao.saveContractRefs(contractRefs);
    }

    public void saveContractor(List<CurrentContractor> currentContractors) {
        log.debug("Save contractors: {}", currentContractors);
        postgresPartyDao.saveContractor(currentContractors);
    }

    public Shop getShop(GeneralKey generalKey) {
        Shop shop = postgresPartyDao.getShopForUpdate(generalKey.getPartyId(), generalKey.getRefId());
        log.debug("Get shop from DB by partyId={}, shopId={}: {}", generalKey.getPartyId(), generalKey.getRefId(), shop);
        return shop;
    }

    public CurrentContractor getCurrentContractor(String  contractorId) {
        CurrentContractor contractorForUpdate = postgresPartyDao.getContractorForUpdate(contractorId);
        log.debug("Get CurrentContractor from DB by contractorId={}: {}", contractorId, contractorForUpdate);
        return contractorForUpdate;
    }

    public void saveShop(List<Shop> shopList) {
        log.debug("Save shops: {}", shopList);
        postgresPartyDao.saveShop(shopList);
    }

}
