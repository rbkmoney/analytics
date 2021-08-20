package com.rbkmoney.analytics.utils.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.Month;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PaymentsWithFeeConstants {

    public static final String PARTY_ID = "ca2e9162-eda2-4d17-bbfa-dc5e39b1772x";
    public static final String FIRST_SHOP_ID = "ad8b7bfd-0760-4781-a400-51903ee8e510";
    public static final String SECOND_SHOP_ID = "ad8b7bfd-0760-4781-a400-51903ee8e511";
    public static final String THIRD_SHOP_ID = "ad8b7bfd-0760-4781-a400-51903ee8e512";
    public static final LocalDateTime BEFORE_FIRST_TIMESTAMP = LocalDateTime.of(2019, Month.DECEMBER, 4, 0, 0);
    public static final LocalDateTime BEFORE_SECOND_TIMESTAMP = BEFORE_FIRST_TIMESTAMP.plusDays(1);
    public static final LocalDateTime BEFORE_THIRD_TIMESTAMP = BEFORE_SECOND_TIMESTAMP.plusDays(1);
    public static final LocalDateTime AFTER_THIRD_TIMESTAMP = BEFORE_THIRD_TIMESTAMP.plusDays(1);

}
