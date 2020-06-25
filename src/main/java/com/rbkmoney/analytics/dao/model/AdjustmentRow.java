package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.AdjustmentStatus;
import com.rbkmoney.analytics.domain.CashFlowResult;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class AdjustmentRow extends InvoiceBaseRow {

    private AdjustmentStatus status;
    private String adjustmentId;
    private String paymentId;

    private CashFlowResult oldCashFlowResult;

}