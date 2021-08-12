package com.rbkmoney.analytics.resource.handler;

import com.rbkmoney.analytics.domain.db.tables.pojos.Category;
import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.repository.PostgresRepositoryTest;
import com.rbkmoney.damsel.analytics.search.*;
import com.rbkmoney.geck.common.util.TypeUtil;
import io.github.benas.randombeans.api.EnhancedRandom;
import lombok.Data;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.List;

public class SearchHandlerTest extends PostgresRepositoryTest {

    @Autowired
    private SearchHandler searchHandler;

    @Before
    public void setUp() throws Exception {
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.category;");
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.party;");
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.shop;");
        postgresJdbcTemplate.execute("TRUNCATE TABLE analytics.contract;");
    }

    @Test
    public void testFindPartyIds() throws TException {
        InitialDataHolder initialDataHolder = initialData();
        PartyFilterRequest partyFilterRequest = new PartyFilterRequest()
                .setPartyFilter(new PartyFilter().setContactInfoEmail(initialDataHolder.party.getEmail()))
                .setShopFilter(
                        new ShopFilter()
                                .setLocationUrl(initialDataHolder.shop.getLocationUrl())
                                .setCategoryFilter(
                                        new CategoryFilter().setName(initialDataHolder.getCategory().getName())
                                )
                )
                .setContractFilter(
                        new ContractFilter()
                                .setLegalAgreementSignedAt(
                                        TypeUtil.temporalToString(
                                                initialDataHolder.getContract().getLegalAgreementSignedAt())
                                )
                );
        List<String> partyIds = searchHandler.findPartyIds(partyFilterRequest);
        Assert.assertFalse(partyIds.isEmpty());
    }

    private InitialDataHolder initialData() {
        Party party = EnhancedRandom.random(Party.class);
        partyDao.saveParty(party);
        Category category = EnhancedRandom.random(Category.class);
        categoryDao.saveCategory(category);
        Contract contract = EnhancedRandom.random(Contract.class);
        contract.setPartyId(party.getPartyId());
        contract.setLegalAgreementSignedAt(LocalDateTime.now());
        contractDao.saveContract(contract);
        Shop shop = EnhancedRandom.random(Shop.class);
        shop.setPartyId(party.getPartyId());
        shop.setCategoryId(category.getCategoryId());
        shopDao.saveShop(shop);

        return new InitialDataHolder(party, shop, contract, category);
    }

    @Data
    private class InitialDataHolder {
        private final Party party;
        private final Shop shop;
        private final Contract contract;
        private final Category category;
    }

}
