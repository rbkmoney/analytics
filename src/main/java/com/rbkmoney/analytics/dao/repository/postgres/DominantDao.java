package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.dao.impl.AbstractGenericDao;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static com.rbkmoney.analytics.domain.db.Tables.CATEGORY;

@Component
public class DominantDao extends AbstractGenericDao {

    public DominantDao(DataSource postgresDatasource) {
        super(postgresDatasource); }

    public Long getLastVersion() {
        Query query = getDslContext().select(DSL.max(DSL.field("version_id"))).from(
                getDslContext().select(DSL.max(CATEGORY.VERSION_ID).as("version_id")).from(CATEGORY)
        );
        return fetchOne(query, Long.class);
    }

}
