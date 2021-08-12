package com.rbkmoney.analytics.dao.repository.postgres.party.management.model;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class PartyFilter {
    private final String email;
    private final ShopFilter shopFilter;
    private final ContractFilter contractFilter;
}
