package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.service.model.ShopKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalStorage {

    private Map<String, Party> partyStore = new HashMap<>();

    private Map<ShopKey, Shop> shopStore = new HashMap<>();

    public Party getParty(String partyId) {
        return partyStore.get(partyId);
    }

    public void putParty(String partyId, Party party) {
        partyStore.put(partyId, party);
    }

    public Shop getShop(ShopKey shopKey) {
        return shopStore.get(shopKey);
    }

    public void putShop(ShopKey shopKey, Shop shop) {
        shopStore.put(shopKey, shop);
    }

    public List<Party> getParties() {
        return new ArrayList<>(partyStore.values());
    }

    public List<Shop> getShops() {
        return new ArrayList<>(shopStore.values());
    }

}
