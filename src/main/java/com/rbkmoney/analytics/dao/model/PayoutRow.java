package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.PayoutAccountType;
import com.rbkmoney.analytics.constant.PayoutStatus;
import com.rbkmoney.analytics.constant.PayoutType;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDateTime;

@Data
@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class PayoutRow {

    private String payoutId;
    private PayoutStatus status;
    private PayoutType payoutType;
    private String statusCancelledDetails;
    private boolean isCancelledAfterBeingPaid;

    private LocalDateTime eventTime;
    private LocalDateTime payoutTime;

    private String shopId;
    private String partyId;
    private String contractId;

    private Long amount;
    private Long fee;
    private String currency;

    private String walletId;

    private PayoutAccountType accountType;
    private String purpose;
    private LocalDateTime legalAgreementSignedAt;
    private String legalAgreementId;
    private LocalDateTime legalAgreementValidUntil;

    private String russianAccount;
    private String russianBankName;
    private String russianBankPostAccount;
    private String russianBankBik;
    private String russianInn;

    private String internationalAccountHolder;
    private String internationalBankName;
    private String internationalBankAddress;
    private String internationalIban;
    private String internationalBic;
    private String internationalLocalBankCode;
    private String internationalLegalEntityLegalName;
    private String internationalLegalEntityTradingName;
    private String internationalLegalEntityRegisteredAddress;
    private String internationalLegalEntityActualAddress;
    private String internationalLegalEntityRegisteredNumber;
    private String internationalBankNumber;
    private String internationalBankAbaRtn;
    private String internationalBankCountryCode;
    private String internationalCorrespondentBankNumber;
    private String internationalCorrespondentBankAccount;
    private String internationalCorrespondentBankName;
    private String internationalCorrespondentBankAddress;
    private String internationalCorrespondentBankBic;
    private String internationalCorrespondentBankIban;
    private String internationalCorrespondentBankAbaRtn;
    private String internationalCorrespondentBankCountryCode;
}
