CREATE DATABASE IF NOT EXISTS analytic;

DROP TABLE IF EXISTS analytic.events_sink;

create table analytic.events_sink
(
    timestamp             Date,
    eventTime             UInt64,
    eventTimeHour         UInt64,

    partyId               String,
    shopId                String,

    email                 String,
    providerName          String,

    amount                UInt64,
    guaranteeDeposit      UInt64,
    systemFee             UInt64,
    providerFee           UInt64,
    externalFee           UInt64,
    currency              String,

    status                Enum8('pending' = 1, 'processed' = 2, 'captured' = 3, 'cancelled' = 4, 'failed' = 5),
    errorReason           String,
    errorCode             String,

    invoiceId             String,
    paymentId             String,
    sequenceId            UInt64,

    ip                    String,
    bin                   String,
    maskedPan             String,
    paymentTool           String,
    fingerprint           String,
    cardToken             String,
    paymentSystem         String,
    digitalWalletProvider String,
    digitalWalletToken    String,
    cryptoCurrency        String,
    mobileOperator        String,

    paymentCountry        String,
    bankCountry           String,

    paymentTime           UInt64,
    providerId            String,
    terminal              String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, paymentTool, status, currency, providerName, fingerprint, cardToken, invoiceId, paymentId, sequenceId);

DROP TABLE IF EXISTS analytic.events_sink_refund;

create table analytic.events_sink_refund
(
    timestamp             Date,
    eventTime             UInt64,
    eventTimeHour         UInt64,

    partyId               String,
    shopId                String,

    email                 String,
    providerName          String,

    amount                UInt64,
    guaranteeDeposit      UInt64,
    systemFee             UInt64,
    providerFee           UInt64,
    externalFee           UInt64,
    currency              String,

    reason                String,

    status                Enum8('pending' = 1, 'succeeded' = 2, 'failed' = 3),
    errorReason           String,
    errorCode             String,

    invoiceId             String,
    refundId              String,
    paymentId             String,
    sequenceId            UInt64,

    ip                    String,
    fingerprint           String,
    cardToken             String,
    paymentSystem         String,
    digitalWalletProvider String,
    digitalWalletToken    String,
    cryptoCurrency        String,
    mobileOperator        String,

    paymentCountry        String,
    bankCountry           String,

    paymentTime           UInt64,
    providerId            String,
    terminal              String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, status, currency, providerName, fingerprint, cardToken, invoiceId, paymentId, refundId, sequenceId);

DROP TABLE IF EXISTS analytic.events_sink_adjustment;

create table analytic.events_sink_adjustment
(
    timestamp             Date,
    eventTime             UInt64,
    eventTimeHour         UInt64,

    partyId               String,
    shopId                String,

    email                 String,
    providerName          String,

    amount                UInt64,
    guaranteeDeposit      UInt64,
    systemFee             UInt64,
    providerFee           UInt64,
    externalFee           UInt64,

    oldAmount             UInt64,
    oldGuaranteeDeposit   UInt64,
    oldSystemFee          UInt64,
    oldProviderFee        UInt64,
    oldExternalFee        UInt64,

    currency              String,

    reason                String,

    status                Enum8('captured' = 1, 'cancelled' = 2),
    errorCode             String,
    errorReason           String,

    invoiceId             String,
    adjustmentId          String,
    paymentId             String,
    sequenceId            UInt64,

    ip                    String,
    fingerprint           String,
    cardToken             String,
    paymentSystem         String,
    digitalWalletProvider String,
    digitalWalletToken    String,
    cryptoCurrency        String,
    mobileOperator        String,

    paymentCountry        String,
    bankCountry           String,

    paymentTime           UInt64,
    providerId            String,
    terminal              String

) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, status, currency, providerName, fingerprint, cardToken, invoiceId, paymentId, adjustmentId, sequenceId);

DROP TABLE IF EXISTS analytic.events_sink_chargeback;

create table analytic.events_sink_chargeback
(
    timestamp             Date,
    eventTime             UInt64,
    eventTimeHour         UInt64,

    partyId               String,
    shopId                String,

    email                 String,
    providerName          String,

    amount                UInt64,
    guaranteeDeposit      UInt64,
    systemFee             UInt64,
    providerFee           UInt64,
    externalFee           UInt64,

    currency              String,

    chargebackCode        String,

    stage                 Enum8('chargeback' = 1, 'pre_arbitration' = 2, 'arbitration' = 3),
    status                Enum8('accepted' = 1, 'rejected' = 2, 'cancelled' = 3),
    category              Enum8('fraud' = 1, 'dispute' = 2, 'authorisation' = 3, 'processing_error' = 4),

    invoiceId             String,
    chargebackId          String,
    paymentId             String,
    sequenceId            UInt64,

    ip                    String,
    fingerprint           String,
    cardToken             String,
    paymentSystem         String,
    digitalWalletProvider String,
    digitalWalletToken    String,
    cryptoCurrency        String,
    mobileOperator        String,

    paymentCountry        String,
    bankCountry           String,

    paymentTime           UInt64,
    providerId            String,
    terminal              String

) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, category, status, stage, currency, providerName, fingerprint, cardToken,
invoiceId, paymentId, chargebackId, sequenceId);

DROP TABLE IF EXISTS analytic.events_sink_payout;

CREATE TABLE analytic.events_sink_payout
(
    payoutId                                  String,
    status                                    Enum8('unpaid' = 1, 'paid' = 2, 'cancelled' = 3, 'confirmed' = 4),
    payoutType                                Enum8('bank_account' = 1, 'wallet' = 2),
    statusCancelledDetails                    String,
    isCancelledAfterBeingPaid                 UInt8,

    timestamp                                 Date,
    eventTime                                 UInt64,
    eventTimeHour                             UInt64,
    payoutTime                                UInt64,

    shopId                                    String,
    partyId                                   String,
    contractId                                String,

    amount                                    UInt64,
    fee                                       UInt64,
    currency                                  String,

    walletId                                  String,

    accountType                               Enum8('russian_payout_account' = 1, 'international_payout_account' = 2),
    purpose                                   String,
    legalAgreementSignedAt                    UInt64,
    legalAgreementId                          String,
    legalAgreementValidUntil                  UInt64,

    russianAccount                            String,
    russianBankName                           String,
    russianBankPostAccount                    String,
    russianBankBik                            String,
    russianInn                                String,

    internationalAccountHolder                String,
    internationalBankName                     String,
    internationalBankAddress                  String,
    internationalIban                         String,
    internationalBic                          String,
    internationalLocalBankCode                String,
    internationalLegalEntityLegalName         String,
    internationalLegalEntityTradingName       String,
    internationalLegalEntityRegisteredAddress String,
    internationalLegalEntityActualAddress     String,
    internationalLegalEntityRegisteredNumber  String,
    internationalBankNumber                   String,
    internationalBankAbaRtn                   String,
    internationalBankCountryCode              String,
    internationalCorrespondentBankNumber      String,
    internationalCorrespondentBankAccount     String,
    internationalCorrespondentBankName        String,
    internationalCorrespondentBankAddress     String,
    internationalCorrespondentBankBic         String,
    internationalCorrespondentBankIban        String,
    internationalCorrespondentBankAbaRtn      String,
    internationalCorrespondentBankCountryCode String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, status, payoutId, currency, accountType, payoutType, contractId, walletId);

CREATE TABLE analytic.events_sink_party
(
    timestamp                Date,
    eventTime                UInt64,
    eventTimeHour            UInt64,

    partyId                  String,
    createdAt                UInt64,
    email                    String,
    blocking                 Enum8('unblocked' = 1, 'blocked' = 2),
    blockedReason            String,
    blockedSince             UInt64,
    unblockedReason          String,
    unblockedSince           UInt64,
    suspension               Enum8('active' = 1, 'suspended' = 2),
    suspensionActiveSince    UInt64,
    suspensionSuspendedSince UInt64,
    revisionId               String,
    revisionChangedAt        UInt64

) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, blocking, suspension);

CREATE TABLE analytic.events_sink_contractor
(
    timestamp                                 Date,
    eventTime                                 UInt64,
    eventTimeHour                             UInt64,

    partyId                                   String,
    contractorId                              String,
    contractorType                            Enum8('registered_user' = 1, 'legal_entity' = 2, 'private_entity' = 3),
    regUserEmail                              String,
    legalEntityType                           Nullable(Enum8('russian_legal_entity' = 1, 'international_legal_entity' = 2)),
    russianLegalEntityName                    String,
    russianLegalEntityRegisteredNumber        String,
    russianLegalEntityInn                     String,
    russianLegalEntityActualAddress           String,
    russianLegalEntityPostAddress             String,
    russianLegalEntityRepresentativePosition  String,
    russianLegalEntityRepresentativeFullName  String,
    russianLegalEntityRepresentativeDocument  String,
    russianLegalEntityBankAccount             String,
    russianLegalEntityBankName                String,
    russianLegalEntityBankPostAccount         String,
    russianLegalEntityBankBik                 String,

    internationalLegalEntityName              String,
    internationalLegalEntityTradingName       String,
    internationalLegalEntityRegisteredAddress String,
    internationalLegalEntityRegisteredNumber  String,

    privateEntityType                         Nullable(Enum8('russian_private_entity' = 1)),
    russianPrivateEntityFirstName             String,
    russianPrivateEntitySecondName            String,
    russianPrivateEntityMiddleName            String,
    russianPrivateEntityPhoneNumber           String,
    russianPrivateEntityEmail                 String,

    contractorIdentificationLevel             Nullable(Enum8('none' = 1, 'partial' = 2, 'full' = 3))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, contractorId, contractorType);

CREATE TABLE analytic.events_sink_shop
(
    timestamp                Date,
    eventTime                UInt64,
    eventTimeHour            UInt64,

    partyId                  String,
    shopId                   String,
    categoryId               String,
    contractId               String,
    payoutToolId             String,
    payoutScheduleId         String,
    createdAt                UInt64,
    blocking                 Enum8('unblocked' = 1, 'blocked' = 2),
    blockedReason            String,
    blockedSince             UInt64,
    unblockedReason          String,
    unblockedSince           UInt64,
    suspension               Enum8('active' = 1, 'suspended' = 2),
    suspensionActiveSince    UInt64,
    suspensionSuspendedSince UInt64,
    detailsName              String,
    detailsDescription       String,
    locationUrl              String,
    accountCurrencyCode      String,
    accountSettlement        String,
    accountGuarantee         String,
    accountPayout            String

) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, categoryId, contractId, payoutToolId, payoutScheduleId, blocking, suspension)
