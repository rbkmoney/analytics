package com.rbkmoney.analytics.dao.repository.postgres.party.management.model;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ShopFilter {
    private final String locationUrl;
    private final String categoryName;
}
