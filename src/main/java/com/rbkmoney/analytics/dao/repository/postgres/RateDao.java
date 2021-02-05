package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.domain.db.tables.pojos.Rate;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.List;
import java.util.stream.Collectors;

import static com.rbkmoney.analytics.domain.db.Tables.RATE;

@Component
public class RateDao extends AbstractGenericDao {

    private final RowMapper<Rate> rateRowMapper;

    public RateDao(DataSource postgresDatasource) {
        super(postgresDatasource);
        this.rateRowMapper = new RecordRowMapper<>(RATE, Rate.class);
    }

    public void saveRateBatch(List<Rate> rates) {
        List<Query> queries = rates.stream()
                .map(rate -> getDslContext().newRecord(RATE, rate))
                .map(rateRecord -> getDslContext()
                        .insertInto(RATE).set(rateRecord)
                        .onConflict(RATE.SOURCE_ID, RATE.SOURCE_SYMBOLIC_CODE, RATE.DESTINATION_SYMBOLIC_CODE,
                                RATE.LOWER_BOUND_INCLUSIVE, RATE.UPPER_BOUND_EXCLUSIVE)
                        .doNothing())
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    public Rate getRate(String sourceId, String sourceCode, String destinationCode) {
        Query query = getDslContext().selectFrom(RATE)
                .where(RATE.SOURCE_ID.eq(sourceId))
                .and(RATE.SOURCE_SYMBOLIC_CODE.eq(sourceCode)
                        .and(RATE.DESTINATION_SYMBOLIC_CODE.eq(destinationCode)));
        return fetchOne(query, rateRowMapper);
    }

}
