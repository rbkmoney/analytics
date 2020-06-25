package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.PayoutStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class PayoutRow {

    private PayoutStatus status;

    // TODO [a.romanov]: more fields
}