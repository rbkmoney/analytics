package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.domain.db.tables.records.PartyRecord;
import com.rbkmoney.analytics.domain.db.tables.records.ShopRecord;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.rbkmoney.analytics.domain.db.Tables.PARTY;
import static com.rbkmoney.analytics.domain.db.Tables.SHOP;

@Component
public class PostgresPartyDao extends AbstractGenericDao {

    private final RowMapper<Party> partyRowMapper;

    private final RowMapper<Shop> shopRowMapper;

    public PostgresPartyDao(DataSource postgresDatasource) {
        super(postgresDatasource);
        this.partyRowMapper = new RecordRowMapper<>(PARTY, Party.class);
        this.shopRowMapper = new RecordRowMapper<>(SHOP, Shop.class);
    }

    public void saveParty(Party party) {
        PartyRecord partyRecord = getDslContext().newRecord(PARTY, party);
        Query query = getDslContext()
                .insertInto(PARTY).set(partyRecord)
                .onConflict(PARTY.PARTY_ID)
                .doUpdate()
                .set(partyRecord);
        execute(query);
    }

    public void saveParty(List<Party> partyList) {
        List<Query> queries = partyList.stream()
                .map(party -> getDslContext().newRecord(PARTY, party))
                .map(partyRecord -> {
                    return getDslContext()
                            .insertInto(PARTY).set(partyRecord)
                            .onConflict(PARTY.PARTY_ID)
                            .doUpdate()
                            .set(partyRecord);
                })
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    public Party getPartyForUpdate(String partyId) {
        Query query = getDslContext().selectFrom(PARTY)
                .where(PARTY.PARTY_ID.eq(partyId))
                .forUpdate();
        return fetchOne(query, partyRowMapper);
    }

    public void saveShop(Shop shop) {
        ShopRecord shopRecord = getDslContext().newRecord(SHOP, shop);
        Query query = getDslContext()
                .insertInto(SHOP).set(shopRecord)
                .onConflict(SHOP.PARTY_ID, SHOP.SHOP_ID)
                .doUpdate()
                .set(shopRecord);
        execute(query);
    }

    public void saveShop(List<Shop> shops) {
        List<Query> queries = shops.stream()
                .map(shop -> getDslContext().newRecord(SHOP, shop))
                .map(shopRecord -> {
                    return getDslContext()
                            .insertInto(SHOP).set(shopRecord)
                            .onConflict(SHOP.PARTY_ID, SHOP.SHOP_ID)
                            .doUpdate()
                            .set(shopRecord);
                })
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    public Shop getShopForUpdate(String partyId, String shopId) {
        Query query = getDslContext().selectFrom(SHOP)
                .where(SHOP.PARTY_ID.eq(partyId).and(SHOP.SHOP_ID.eq(shopId)))
                .forUpdate();
        return fetchOne(query, shopRowMapper);
    }


}
