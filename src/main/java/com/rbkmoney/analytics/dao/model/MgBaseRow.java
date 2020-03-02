package com.rbkmoney.analytics.dao.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;

@Data
@NoArgsConstructor
public class MgBaseRow {

    private Date timestamp;
    private Long eventTime;
    private Long eventTimeHour;

    private String ip;
    private String email;
    private String fingerprint;
    private String cardToken;
    private String paymentSystem;
    private String digitalWalletProvider;
    private String digitalWalletToken;
    private String cryptoCurrency;
    private String mobileOperator;

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
    private String paymentId;

    private Long sequenceId;

}