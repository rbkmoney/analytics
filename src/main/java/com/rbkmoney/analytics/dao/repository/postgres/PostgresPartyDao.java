package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.domain.db.tables.records.PartyRecord;
import com.rbkmoney.analytics.domain.db.tables.records.ShopRecord;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static com.rbkmoney.analytics.domain.db.Tables.*;

@Component
public class PostgresPartyDao extends AbstractGenericDao {

    private final RowMapper<Party> partyRowMapper;

    private final RowMapper<Shop> shopRowMapper;

    public static final String UPSERT = "INSERT INTO analytic.events_sink_party ( " +
            " party_id, created_at, email, blocking, blocked_reason, blocked_since, unblocked_reason, unblocked_since," +
            " suspension, suspension_active_since, suspension_suspended_since, revision_id, revision_changed_at, " +
            " contractor_id, contractor_type, reg_user_email, legal_entity_type, russian_legal_entity_name, russian_legal_entity_registered_number, " +
            " russian_legal_entity_inn, russian_legal_entity_actual_address, russian_legal_entity_post_address, russian_legal_entity_representative_position, " +
            " russian_legal_entity_representative_full_name, russian_legal_entity_representative_document, russian_legal_entity_bank_account, " +
            " russian_legal_entity_bank_name, russian_legal_entity_bank_post_account, russian_legal_entity_bank_bik, international_legal_entity_name, " +
            " international_legal_entity_trading_name, international_legal_entity_registered_address, international_legal_entity_registered_number, " +
            " private_entity_type, russian_private_entity_first_name, russian_private_entity_second_name, russian_private_entity_middle_name, " +
            " russian_private_entity_phone_number, russian_private_entity_email, contractor_identification_level)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(party_id) DO UPDATE SET" +
            " created_at = excluded.created_at, email = excluded.email, blocking = excluded.blocking, blocked_reason = excluded.blocked_reason, unblocked_since = excluded.unblocked_since, " +
            " suspension = excluded.suspension, suspension_active_since = excluded.suspension_active_since, suspension_suspended_since = excluded.suspension_suspended_since, " +
            " revision_id = excluded.revision_id, revision_changed_at = excluded.revision_changed_at, contractor_id = excluded.contractor_id, contractor_type = excluded.contractor_type, " +
            " reg_user_email = excluded.reg_user_email, legal_entity_type = excluded.legal_entity_type, russian_legal_entity_name = excluded.russian_legal_entity_name, " +
            " russian_legal_entity_registered_number = excluded.russian_legal_entity_registered_number, russian_legal_entity_inn = excluded.russian_legal_entity_inn, russian_legal_entity_actual_address = excluded.russian_legal_entity_actual_address, " +
            " russian_legal_entity_post_address = excluded.russian_legal_entity_post_address, russian_legal_entity_representative_position = excluded.russian_legal_entity_representative_position, " +
            " ";

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

    public Party getParty(String partyId) {
        Query query = getDslContext().selectFrom(PARTY)
                .where(PARTY.PARTY_ID.eq(partyId));
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

    public Shop getShop(String partyId, String shopId) {
        Query query = getDslContext().selectFrom(SHOP)
                .where(SHOP.PARTY_ID.eq(partyId).and(SHOP.SHOP_ID.eq(shopId)));
        return fetchOne(query, shopRowMapper);
    }


}
