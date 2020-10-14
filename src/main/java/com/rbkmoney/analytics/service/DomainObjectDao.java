package com.rbkmoney.analytics.service;

import com.rbkmoney.dao.DaoException;

public interface DomainObjectDao<T> {

    Long save(T domainObject) throws DaoException;

}
