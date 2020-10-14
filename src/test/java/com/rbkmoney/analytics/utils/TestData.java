package com.rbkmoney.analytics.utils;

import com.rbkmoney.damsel.domain.*;
import com.rbkmoney.damsel.domain_config.*;

import java.util.List;

public class TestData {

    public static Commit buildInsertCategoryCommit(Integer id, String name, String description, CategoryType categoryType) {
        Operation operation = new Operation();
        InsertOp insertOp = new InsertOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCategory(buildCategoryObject(id, name, description, categoryType));
        insertOp.setObject(domainObject);
        operation.setInsert(insertOp);

        return new Commit(List.of(operation));
    }

    public static Commit buildUpdateCategoryCommit(Integer id,
                                             String name,
                                             String description,
                                             CategoryType categoryType,
                                             DomainObject oldObject) {
        Operation operation = new Operation();
        UpdateOp updateOp = new UpdateOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCategory(buildCategoryObject(id, name, description, categoryType));
        updateOp.setNewObject(domainObject);
        updateOp.setOldObject(oldObject);
        operation.setUpdate(updateOp);

        return new Commit(List.of(operation));
    }

    public static Commit buildRemoveCategoryCommit(Integer id, String name, String description, CategoryType categoryType) {
        Operation operation = new Operation();
        RemoveOp removeOp = new RemoveOp();
        DomainObject domainObject = new DomainObject();
        domainObject.setCategory(buildCategoryObject(id, name, description, categoryType));
        removeOp.setObject(domainObject);
        operation.setRemove(removeOp);

        return new Commit(List.of(operation));
    }

    public static CategoryObject buildCategoryObject(Integer id, String name, String description, CategoryType categoryType) {
        CategoryObject categoryObject = new CategoryObject();
        Category category = new Category();
        category.setName(name);
        category.setDescription(description);
        category.setType(categoryType);
        categoryObject.setData(category);
        CategoryRef categoryRef = new CategoryRef();
        categoryRef.setId(id);
        categoryObject.setRef(categoryRef);

        return categoryObject;
    }

}
