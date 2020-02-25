package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.PaymentStatus;
import com.rbkmoney.analytics.constant.PaymentToolType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;

@Data
@NoArgsConstructor
public class MgPaymentSinkRow {

    private Date timestamp;
    private Long eventTime;
    private Long eventTimeHour;

    private String ip;
    private String email;
    private String bin;
    private String fingerprint;

    private String shopId;
    private String partyId;

    private PaymentStatus status;
    private String errorCode;

    private Long accountId;

    private long totalAmount;
    private long merchantAmount;
    private long guaranteeDeposit;
    private long systemFee;
    private long providerFee;
    private long externalFee;

    private String currency;

    private String ipCountry;
    private String bankCountry;
    private String maskedPan;
    private String provider;
    private PaymentToolType paymentTool;

    private String invoiceId;
    private String paymentId;
    private Long sequenceId;

}
