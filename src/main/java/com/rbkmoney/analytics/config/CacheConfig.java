package com.rbkmoney.analytics.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.service.model.ShopKey;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CacheConfig {

    @Bean
    public Cache<String, Party> partyCache(@Value("${cache.party.size}") int cacheSize) {
        return Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    @Bean
    public Cache<ShopKey, Shop> shopCache(@Value("${cache.shop.size}") int cacheSize) {
        return Caffeine.newBuilder().maximumSize(cacheSize).build();
    }
}
