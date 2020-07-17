package com.rbkmoney.analytics.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresPartyDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PartyService {

    private final PostgresPartyDao postgresPartyDao;

    private final Cache<String, Party> partyCache;

    private final Cache<String, Shop> shopCache;

    public Party getParty(String partyId) {
        Party party = partyCache.getIfPresent(partyId);
        if (party != null) {
            return copy(party);
        }
        Party partyFromDb = postgresPartyDao.getParty(partyId);
        if (partyFromDb == null) {
            throw new IllegalStateException(String.format("Party not found. partyId=%s", partyId));
        }

        return partyFromDb;
    }

    public void saveParty(Party party) {
        postgresPartyDao.saveParty(party);
        partyCache.put(party.getPartyId(), party);
    }

    public Shop getShop(String partyId, String shopId) {
        String key = partyId + shopId;
        Shop shop = shopCache.getIfPresent(key);
        if (shop != null) {
            return copy(shop);
        }
        Shop shopFromDb = postgresPartyDao.getShop(partyId, shopId);
        if (shopFromDb == null) {
            throw new IllegalStateException(String.format("Shop not found. partyId=%s; shopId=%s", partyId, shopId));
        }

        return shopFromDb;
    }

    public void saveShop(Shop shop) {
        postgresPartyDao.saveShop(shop);
        String key = shop.getPartyId() + shop.getShopId();
        shopCache.put(key, shop);
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
