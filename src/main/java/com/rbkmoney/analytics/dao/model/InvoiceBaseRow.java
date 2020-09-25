package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.PaymentToolType;
import com.rbkmoney.analytics.domain.CashFlowResult;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
public class InvoiceBaseRow {

    private LocalDateTime eventTime;
    private LocalDateTime paymentTime;

    private String ip;
    private String email;
    private String fingerprint;
    private String cardToken;
    private String cardHolderName;
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
    private Integer providerId;
    private Integer terminal;

    private String invoiceId;
    private String paymentId;

    private Long sequenceId;

    private String paymentCountry;
    private String bankCountry;

    private PaymentToolType paymentTool;
    private String bin;

    private String maskedPan;

    private String errorCode;
    private String errorReason;

}
