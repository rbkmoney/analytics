package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.AdjustmentStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class MgAdjustmentRow extends MgBaseRow {

    private AdjustmentStatus status;
    private String errorCode;
    private String adjustmentId;
    private String paymentId;

}