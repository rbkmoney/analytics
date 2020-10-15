package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.domain.db.tables.pojos.Dominant;
import com.rbkmoney.analytics.domain.db.tables.records.DominantRecord;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static com.rbkmoney.analytics.domain.db.Tables.CATEGORY;
import static com.rbkmoney.analytics.domain.db.Tables.DOMINANT;

@Component
public class DominantDao extends AbstractGenericDao {

    private final RowMapper<Dominant> dominantRowMapper;

    public DominantDao(DataSource postgresDatasource) {
        super(postgresDatasource);
        this.dominantRowMapper = new RecordRowMapper<>(DOMINANT, Dominant.class);
    }

    public Long getLastVersion() {
        Query query = getDslContext().select(DSL.max(DSL.field("version_id"))).from(
                getDslContext().select(DSL.max(CATEGORY.VERSION_ID).as("version_id")).from(CATEGORY)
        );
        return fetchOne(query, Long.class);
    }

}
