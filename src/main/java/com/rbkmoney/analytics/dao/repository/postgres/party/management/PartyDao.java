package com.rbkmoney.analytics.dao.repository.postgres.party.management;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.records.PartyRecord;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;
import java.util.stream.Collectors;

import static com.rbkmoney.analytics.domain.db.Tables.PARTY;

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

}
