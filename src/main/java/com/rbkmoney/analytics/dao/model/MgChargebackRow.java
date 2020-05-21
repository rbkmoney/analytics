package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.ChargebackCategory;
import com.rbkmoney.analytics.constant.ChargebackStage;
import com.rbkmoney.analytics.constant.ChargebackStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class MgChargebackRow extends MgBaseRow {

    private ChargebackStatus status;
    private String chargebackId;

    private ChargebackCategory category;
    private String chargebackCode;
    private ChargebackStage stage;

}