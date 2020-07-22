package com.rbkmoney.analytics.service;

import com.rbkmoney.analytics.dao.repository.postgres.PostgresPartyDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.service.model.ShopKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PartyService {

    private final PostgresPartyDao postgresPartyDao;

    public Party getParty(String partyId) {
        Party party = postgresPartyDao.getPartyForUpdate(partyId);
        log.debug("Get party from DB by partyId={}. Result={}", partyId, party);

        return party;
    }

    public void saveParty(Party party) {
        log.debug("Save party: {}", party);
        postgresPartyDao.saveParty(party);
    }

    public void saveParty(List<Party> partyList) {
        log.debug("Save parties: size={}", partyList.size());
        postgresPartyDao.saveParty(partyList);
    }

    public Shop getShop(ShopKey shopKey) {
        Shop shop = postgresPartyDao.getShopForUpdate(shopKey.getPartyId(), shopKey.getShopId());
        log.debug("Get shop from DB by partyId={}, shopId={}: {}", shopKey.getPartyId(), shopKey.getShopId(), shop);

        return shop;
    }

    public void saveShop(Shop shop) {
        log.debug("Save shop: {}", shop);
        postgresPartyDao.saveShop(shop);
    }

    public void saveShop(List<Shop> shopList) {
        log.debug("Save shops: size={}", shopList.size());
        postgresPartyDao.saveShop(shopList);
    }

    private Party copy(Party party) {
        Party targetParty = new Party();
        BeanUtils.copyProperties(party, targetParty);

        return targetParty;
    }

    private Shop copy(Shop shop) {
        Shop targetShop = new Shop();
        BeanUtils.copyProperties(shop, targetShop);

        return targetShop;
    }


}
