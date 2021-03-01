package com.rbkmoney.analytics.dao.repository.postgres.party.management;

import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.analytics.domain.db.tables.records.ContractRecord;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import java.util.List;

import static com.rbkmoney.analytics.domain.db.tables.Contract.CONTRACT;

@Component
public class ContractDao extends AbstractGenericDao {

    private final RowMapper<Contract> contractRefRowMapper;

    public ContractDao(DataSource postgresDatasource) {
        super(postgresDatasource);
        this.contractRefRowMapper = new RecordRowMapper<>(CONTRACT, Contract.class);
    }

    public void saveContract(Contract contract) {
        final ContractRecord record = getDslContext().newRecord(CONTRACT, contract);
        Query queries = getDslContext()
                .insertInto(CONTRACT).set(record)
                .onConflict(CONTRACT.PARTY_ID, CONTRACT.CONTRACT_ID)
                .doUpdate()
                .set(record);
        execute(queries);
    }

    public Contract getContractByPartyIdAndContractId(String partyId, String contractId) {
        Query query = getDslContext().selectFrom(CONTRACT)
                .where(CONTRACT.CONTRACT_ID.eq(contractId)
                        .and(CONTRACT.PARTY_ID.eq(partyId)));
        return fetchOne(query, contractRefRowMapper);
    }

}
