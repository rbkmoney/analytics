package com.rbkmoney.analytics.dao.repository.postgres;

import com.rbkmoney.analytics.domain.db.tables.pojos.Category;
import com.rbkmoney.analytics.domain.db.tables.records.CategoryRecord;
import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.mapper.RecordRowMapper;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

import static com.rbkmoney.analytics.domain.db.Tables.CATEGORY;

@Component
public class CategoryDao extends AbstractGenericDao {

    private final RowMapper<Category> categoryRowMapper;

    public CategoryDao(DataSource postgresDatasource) {
        super(postgresDatasource);
        this.categoryRowMapper = new RecordRowMapper<>(CATEGORY, Category.class);
    }

    public void saveCategory(Category category) {
        CategoryRecord categoryRecord = getDslContext().newRecord(CATEGORY, category);
        Query query = getDslContext()
                .insertInto(CATEGORY).set(categoryRecord)
                .set(categoryRecord);
        execute(query);
    }

    public void updateCategory(int categoryId, Category category) {
        Query query = getDslContext().update(CATEGORY)
                .set(CATEGORY.VERSION_ID, category.getVersionId())
                .set(CATEGORY.NAME, category.getName())
                .set(CATEGORY.DESCRIPTION, category.getDescription())
                .set(CATEGORY.TYPE, category.getType())
                .where(CATEGORY.CATEGORY_ID.eq(categoryId));
        execute(query);
    }

    public void removeCategory(Category category) {
        Query query = getDslContext().update(CATEGORY)
                .set(CATEGORY.DELETED, true)
                .set(CATEGORY.VERSION_ID, category.getVersionId())
                .where(CATEGORY.CATEGORY_ID.eq(category.getCategoryId()));
        execute(query);
    }

    public Category getCategory(Integer categoryId, Long versionId) {
        Query query = getDslContext().selectFrom(CATEGORY)
                .where(CATEGORY.CATEGORY_ID.eq(categoryId)).and(CATEGORY.VERSION_ID.eq(versionId));

        return fetchOne(query, categoryRowMapper);
    }

}
