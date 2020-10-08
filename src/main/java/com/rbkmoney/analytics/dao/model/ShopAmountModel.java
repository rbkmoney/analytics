package com.rbkmoney.analytics.dao.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ShopAmountModel {
    private Long amount;
    private String shopId;
    private String currency;
}
