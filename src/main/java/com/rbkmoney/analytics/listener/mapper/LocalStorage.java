package com.rbkmoney.analytics.listener.mapper;

import java.util.HashMap;
import java.util.Map;

public class LocalStorage<T> {
    private Map<String, T> storage = new HashMap<>();

    public T get(String key) {
        return storage.get(key);
    }

    public void put(String key, T object) {
        storage.put(key, object);
    }
}
