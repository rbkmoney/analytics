package com.rbkmoney.analytics.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresPartyDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.handler.party.LocalStorage;
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

    private final Cache<String, Party> partyCache;

    private final Cache<ShopKey, Shop> shopCache;

    public Party getParty(String partyId, LocalStorage storage) {
        Party party = storage.getParty(partyId);
        if (party != null) {
            log.debug("Get party from local storage: {}", party);
            return copy(party);
        }
        party = partyCache.getIfPresent(partyId);
        if (party != null) {
            log.debug("Get party from cache: {}", party);
            return copy(party);
        }
        party = postgresPartyDao.getParty(partyId);
        if (party == null) {
            throw new IllegalStateException(String.format("Party not found. partyId=%s", partyId));
        }
        log.debug("Get party from database: {}", party);

        return party;
    }

    public void saveParty(Party party) {
        log.debug("Save party: {}", party);
        postgresPartyDao.saveParty(party);
        partyCache.put(party.getPartyId(), party);
    }

    public void saveParty(List<Party> partyList) {
        log.debug("Save parties: size={}", partyList.size());
        for (Party party : partyList) {
            partyCache.put(party.getPartyId(), party);
        }
        postgresPartyDao.saveParty(partyList);
    }

    public Shop getShop(ShopKey shopKey, LocalStorage storage) {
        Shop shop = storage.getShop(shopKey);
        if (shop != null) {
            log.debug("Get shop from cache: {}", shop);
            return copy(shop);
        }
        shop = shopCache.getIfPresent(shopKey);
        if (shop != null) {
            log.debug("Get shop from cache: {}", shop);
            return copy(shop);
        }
        shop = postgresPartyDao.getShop(shopKey.getPartyId(), shopKey.getShopId());
        if (shop == null) {
            throw new IllegalStateException(String.format("Shop not found. partyId=%s; shopId=%s", shopKey.getPartyId(), shopKey.getShopId()));
        }
        log.debug("Get shop from database: {}", shop);

        return shop;
    }

    public void saveShop(Shop shop) {
        postgresPartyDao.saveShop(shop);
        ShopKey shopKey = new ShopKey(shop.getPartyId(), shop.getShopId());
        shopCache.put(shopKey, shop);
    }

    public void saveShop(List<Shop> shopList) {
        log.debug("Save shops: size={}", shopList.size());
        for (Shop shop : shopList) {
            shopCache.put(new ShopKey(shop.getPartyId(), shop.getShopId()), shop);
        }
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
