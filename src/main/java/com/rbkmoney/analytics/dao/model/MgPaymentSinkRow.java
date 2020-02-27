package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.PaymentToolType;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.sql.Date;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class MgPaymentSinkRow extends MgBaseRow{

    private PaymentStatus status;
    private String errorCode;

    private String ipCountry;
    private String bankCountry;
    private String maskedPan;
    private String provider;

    private String bin;

    private PaymentToolType paymentTool;

}
