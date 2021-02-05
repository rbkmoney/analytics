package com.rbkmoney.analytics.dao.repository.postgres.party.management;

import com.rbkmoney.analytics.domain.db.tables.pojos.Contractor;
import com.rbkmoney.analytics.domain.db.tables.records.ContractorRecord;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;
import java.util.stream.Collectors;

import static com.rbkmoney.analytics.domain.db.Tables.CONTRACTOR;

@Component
public class ContractorDao extends AbstractGenericDao {

    private final RowMapper<Contractor> currentContractorRowMapper;

    public ContractorDao(DataSource postgresDatasource) {
        super(postgresDatasource);
        this.currentContractorRowMapper = new RecordRowMapper<>(CONTRACTOR, Contractor.class);
    }

    public void saveContractor(Contractor currentContractor) {
        final ContractorRecord contractorRecord = getDslContext().newRecord(CONTRACTOR, currentContractor);
        final InsertOnDuplicateSetMoreStep<ContractorRecord> query = getDslContext().insertInto(CONTRACTOR)
                .set(contractorRecord)
                .onConflict(CONTRACTOR.CONTRACTOR_ID)
                .doUpdate()
                .set(contractorRecord);
        execute(query);
    }

    public void saveContractors(List<Contractor> currentContractors) {
        List<Query> queries = currentContractors.stream()
                .map(contractor -> getDslContext().newRecord(CONTRACTOR, contractor))
                .map(currentContractorRecord -> getDslContext()
                        .insertInto(CONTRACTOR).set(currentContractorRecord)
                        .onConflict(CONTRACTOR.CONTRACTOR_ID)
                        .doUpdate()
                        .set(currentContractorRecord))
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    public Contractor getContractorById(String contractorId) {
        Query query = getDslContext().selectFrom(CONTRACTOR)
                .where(CONTRACTOR.CONTRACTOR_ID.eq(contractorId));
        return fetchOne(query, currentContractorRowMapper);
    }

}
