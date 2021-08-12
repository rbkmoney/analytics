package com.rbkmoney.analytics.dao.repository.postgres.party.management.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

@Builder
@Getter
public class ContractFilter {
    private final String legalAgreementSignedAt;
}
