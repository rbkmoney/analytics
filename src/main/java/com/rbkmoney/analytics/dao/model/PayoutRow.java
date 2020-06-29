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
    private String typeAccountPurpose;
    private LocalDateTime typeAccountLegalAgreementSignedAt;
    private String typeAccountLegalAgreementId;
    private LocalDateTime typeAccountLegalAgreementValidUntil;

    private String typeAccountRussianAccount;
    private String typeAccountRussianBankName;
    private String typeAccountRussianBankPostAccount;
    private String typeAccountRussianBankBik;
    private String typeAccountRussianInn;

    private String typeAccountInternationalAccountHolder;
    private String typeAccountInternationalBankName;
    private String typeAccountInternationalBankAddress;
    private String typeAccountInternationalIban;
    private String typeAccountInternationalBic;
    private String typeAccountInternationalLocalBankCode;
    private String typeAccountInternationalLegalEntityLegalName;
    private String typeAccountInternationalLegalEntityTradingName;
    private String typeAccountInternationalLegalEntityRegisteredAddress;
    private String typeAccountInternationalLegalEntityActualAddress;
    private String typeAccountInternationalLegalEntityRegisteredNumber;
    private String typeAccountInternationalBankNumber;
    private String typeAccountInternationalBankAbaRtn;
    private String typeAccountInternationalBankCountryCode;
    private String typeAccountInternationalCorrespondentBankNumber;
    private String typeAccountInternationalCorrespondentBankAccount;
    private String typeAccountInternationalCorrespondentBankName;
    private String typeAccountInternationalCorrespondentBankAddress;
    private String typeAccountInternationalCorrespondentBankBic;
    private String typeAccountInternationalCorrespondentBankIban;
    private String typeAccountInternationalCorrespondentBankAbaRtn;
    private String typeAccountInternationalCorrespondentBankCountryCode;
}