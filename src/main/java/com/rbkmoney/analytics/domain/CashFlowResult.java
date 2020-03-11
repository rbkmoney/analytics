package com.rbkmoney.analytics.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CashFlowResult {

    private final long amount;
    private final long guaranteeDeposit;
    private final long systemFee;
    private final long providerFee;
    private final long externalFee;

}