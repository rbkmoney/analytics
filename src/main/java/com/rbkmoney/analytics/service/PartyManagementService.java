package com.rbkmoney.analytics.service;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ContractorDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.PartyDao;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.ShopDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contractor;
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

    private final PartyDao partyDao;
    private final ShopDao shopDao;
    private final ContractDao contractDao;
    private final ContractorDao contractorDao;

    public Party getParty(String partyId) {
        Party party = partyDao.getPartyById(partyId);
        log.debug("Get party from DB by partyId={}. Result={}", partyId, party);
        return party;
    }

    public Contract getContract(String contractId) {
        Contract contractById = contractDao.getContractById(contractId);
        log.debug("Get contract from DB by contractId={}. Result={}", contractId, contractById);
        return contractById;
    }

    public void saveParty(List<Party> partyList) {
        log.debug("Save parties: {}", partyList);
        partyDao.saveParty(partyList);
    }

    public void saveContractRefs(List<Contract> contracts) {
        log.debug("Save contract: {}", contracts);
        contractDao.saveContract(contracts);
    }

    public void saveContractor(List<Contractor> currentContractors) {
        log.debug("Save contractors: {}", currentContractors);
        contractorDao.saveContractor(currentContractors);
    }

    public Shop getShop(GeneralKey generalKey) {
        Shop shop = shopDao.getShopForUpdate(generalKey.getPartyId(), generalKey.getRefId());
        log.debug("Get shop from DB by partyId={}, shopId={}: {}", generalKey.getPartyId(), generalKey.getRefId(), shop);
        return shop;
    }

    public Contractor getContractorById(String contractorId) {
        Contractor contractorForUpdate = contractorDao.getContractorById(contractorId);
        log.debug("Get CurrentContractor from DB by contractorId={}: {}", contractorId, contractorForUpdate);
        return contractorForUpdate;
    }

    public void saveShop(List<Shop> shopList) {
        log.debug("Save shops: {}", shopList);
        shopDao.saveShop(shopList);
    }

}
