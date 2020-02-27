package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.RefundStatus;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;
import java.util.Map;

@Data
@NoArgsConstructor
public class MgRefundRow {

    private Date timestamp;
    private Long eventTime;
    private Long eventTimeHour;

    private String ip;
    private String email;
    private String fingerprint;

    private RefundStatus status;
    private String errorCode;

    private Long accountId;

    private long totalAmount;
    private long merchantAmount;
    private long guaranteeDeposit;
    private long systemFee;
    private long providerFee;
    private long externalFee;

    private String currency;

    private String shopId;
    private String partyId;

    private String provider;
    private String reason;

    private String invoiceId;
    private String refundId;
    private String paymentId;
    private Long sequenceId;

}