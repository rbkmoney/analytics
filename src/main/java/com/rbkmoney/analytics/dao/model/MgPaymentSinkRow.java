package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.PaymentStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class MgPaymentSinkRow extends MgBaseRow {

    private PaymentStatus status;

}
