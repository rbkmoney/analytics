package com.rbkmoney.analytics.dao.repository.postgres.party.management;

import com.rbkmoney.analytics.domain.db.tables.pojos.Contract;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;
import java.util.stream.Collectors;

import static com.rbkmoney.analytics.domain.db.tables.Contract.CONTRACT;

@Component
public class ContractDao extends AbstractGenericDao {

    private final RowMapper<Contract> contractRefRowMapper;

    public ContractDao(DataSource postgresDatasource) {
        super(postgresDatasource);
        this.contractRefRowMapper = new RecordRowMapper<>(CONTRACT, Contract.class);
    }

    public void saveContract(List<Contract> contractRefs) {
        List<Query> queries = contractRefs.stream()
                .map(party -> getDslContext().newRecord(CONTRACT, party))
                .map(partyRecord -> getDslContext()
                        .insertInto(CONTRACT).set(partyRecord)
                        .onConflict(CONTRACT.ID)
                        .doUpdate()
                        .set(partyRecord))
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    public Contract getContractById(String contractId) {
        Query query = getDslContext().selectFrom(CONTRACT)
                .where(CONTRACT.CONTRACT_ID.eq(contractId));
        return fetchOne(query, contractRefRowMapper);
    }

}
