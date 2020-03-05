package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.domain.CashFlowResult;
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

    private CashFlowResult cashFlowResult;
    private String currency;

    private String shopId;
    private String partyId;

    private String provider;
    private String reason;

    private String invoiceId;
    private String paymentId;

    private Long sequenceId;

    private String paymentCountry;
    private String bankCountry;

}