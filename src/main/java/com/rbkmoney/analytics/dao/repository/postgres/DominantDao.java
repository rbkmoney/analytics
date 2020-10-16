package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.domain.db.tables.pojos.Dominant;
import com.rbkmoney.analytics.domain.db.tables.records.DominantRecord;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import org.jooq.Query;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static com.rbkmoney.analytics.domain.db.Tables.DOMINANT;

@Component
public class DominantDao extends AbstractGenericDao {

    public DominantDao(DataSource postgresDatasource) {
        super(postgresDatasource);
    }

    public void updateVersion(long version, long oldVersion) {
        Query query = getDslContext().update(DOMINANT)
                .set(DOMINANT.LAST_VERSION, version)
                .where(DOMINANT.LAST_VERSION.eq(oldVersion));
        execute(query);
    }

    public void saveVersion(long version) {
        DominantRecord dominantRecord = getDslContext().newRecord(DOMINANT, new Dominant(version));
        Query query = getDslContext()
                .insertInto(DOMINANT)
                .set(dominantRecord);
        execute(query);
    }

    public Long getLastVersion() {
        Query query = getDslContext().select(DOMINANT.LAST_VERSION).from(DOMINANT);

        return fetchOne(query, Long.class);
    }

}
