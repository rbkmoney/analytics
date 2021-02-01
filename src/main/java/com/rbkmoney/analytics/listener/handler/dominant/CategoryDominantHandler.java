package com.rbkmoney.analytics.listener.handler.dominant;

import com.rbkmoney.analytics.dao.repository.postgres.party.management.CategoryDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Category;
import com.rbkmoney.damsel.domain.CategoryObject;
import com.rbkmoney.damsel.domain.DomainObject;
import com.rbkmoney.damsel.domain_config.Operation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class CategoryDominantHandler extends AbstractDominantHandler {

    private final CategoryDao categoryDao;

    @Override
    @Transactional
    public void handle(Operation operation, long versionId) {
        DomainObject dominantObject = getDominantObject(operation);
        CategoryObject category = dominantObject.getCategory();
        if (operation.isSetInsert()) {
            log.info("Save category operation. id='{}' version='{}'", category.getRef().getId(), versionId);
            categoryDao.saveCategory(convertToDatabaseObject(versionId, category));
        } else if (operation.isSetUpdate()) {
            log.info("Update category operation. id='{}' version='{}'", category.getRef().getId(), versionId);
            DomainObject oldObject = operation.getUpdate().getOldObject();
            CategoryObject oldCategory = oldObject.getCategory();
            categoryDao.updateCategory(oldCategory.getRef().getId(), convertToDatabaseObject(versionId, category));
        } else if (operation.isSetRemove()) {
            log.info("Remove category operation. id='{}' version='{}'", category.getRef().getId(), versionId);
            categoryDao.removeCategory(convertToDatabaseObject(versionId, category));
        }
    }

    @Override
    public boolean isHandle(Operation operation) {
        DomainObject dominantObject = getDominantObject(operation);

        return dominantObject.isSetCategory();
    }

    public Category convertToDatabaseObject(long versionId, CategoryObject categoryObject) {
        Category category = new Category();
        category.setVersionId(versionId);
        category.setCategoryId(categoryObject.getRef().getId());
        com.rbkmoney.damsel.domain.Category data = categoryObject.getData();
        category.setName(data.getName());
        category.setDescription(data.getDescription());
        if (data.isSetType()) {
            category.setType(data.getType().name());
        }
        return category;
    }

}
