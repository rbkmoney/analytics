package com.rbkmoney.analytics.flowresolver;

import com.fasterxml.jackson.databind.util.LRUMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrintableLruMap<K, V> extends LRUMap<K, V> {

    public PrintableLruMap(int initialEntries, int maxEntries) {
        super(initialEntries, maxEntries);
    }

    @Override
    public V put(K key, V value) {
        if (_map.size() >= _maxEntries) {
            print();
        }
        return super.put(key, value);
    }

    public void print() {
        log.info("flows: \n");
        _map.entrySet()
                .forEach(kvEntry -> log.info("{}", kvEntry.getValue()));
    }

}
