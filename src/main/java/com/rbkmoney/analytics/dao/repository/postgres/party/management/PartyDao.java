package com.rbkmoney.analytics.dao.repository.postgres.party.management;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.model.PartyFilter;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.records.PartyRecord;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.mapper.RecordRowMapper;
import org.jetbrains.annotations.NotNull;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;
import java.util.stream.Collectors;

import static com.rbkmoney.analytics.domain.db.Tables.*;

@Component
public class PartyDao extends AbstractGenericDao {

    private final RowMapper<Party> partyRowMapper;

    public PartyDao(DataSource postgresDatasource) {
        super(postgresDatasource);
        this.partyRowMapper = new RecordRowMapper<>(PARTY, Party.class);
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
                .map(partyRecord -> getDslContext()
                        .insertInto(PARTY).set(partyRecord)
                        .onConflict(PARTY.PARTY_ID)
                        .doUpdate()
                        .set(partyRecord))
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    public Party getPartyById(String partyId) {
        Query query = getDslContext().selectFrom(PARTY)
                .where(PARTY.PARTY_ID.eq(partyId));
        return fetchOne(query, partyRowMapper);
    }

    public List<Party> getPartyByFilter(PartyFilter filter) {
        SelectWhereStep<?> from = getDslContext().selectFrom(buildSelectFrom(filter));
        Condition condition = DSL.trueCondition();
        if (filter.getEmail() != null) {
            condition = condition.and(PARTY.EMAIL.eq(filter.getEmail()));
        }
        if (filter.getShopFilter() != null && filter.getShopFilter().getLocationUrl() != null) {
            condition = condition.and(
                    SHOP.LOCATION_URL.eq(filter.getShopFilter().getLocationUrl())
            );
        }
        if (filter.getShopFilter() != null && filter.getShopFilter().getCategoryName() != null) {
            condition = condition.and(
                    CATEGORY.NAME.eq(filter.getShopFilter().getCategoryName())
            );
        }
        if (filter.getContractFilter() != null && filter.getContractFilter().getLegalAgreementSignedAt() != null) {
            condition = condition.and(
                    CONTRACT.LEGAL_AGREEMENT_SIGNED_AT.eq(
                            TypeUtil.stringToLocalDateTime(filter.getContractFilter().getLegalAgreementSignedAt())
                    )
            );
        }
        Query query = from.where(condition);

        return fetch(query, partyRowMapper);
    }

    private Table<?> buildSelectFrom(PartyFilter filter) {
        Table<?> from = PARTY;
        if (filter.getShopFilter() != null) {
            from = from.join(SHOP).on(PARTY.PARTY_ID.eq(SHOP.PARTY_ID));
            if (filter.getShopFilter().getCategoryName() != null) {
                from = from.join(CATEGORY).on(SHOP.CATEGORY_ID.eq(CATEGORY.CATEGORY_ID));
            }
        }
        if (filter.getContractFilter() != null) {
            from = from.join(CONTRACT).on(PARTY.PARTY_ID.eq(CONTRACT.PARTY_ID));
        }
        return from;
    }

}
