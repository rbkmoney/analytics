CREATE DATABASE IF NOT EXISTS analytic;

DROP TABLE IF EXISTS analytic.events_sink;

create table analytic.events_sink
(
    timestamp     Date,
    eventTime     UInt64,
    eventTimeHour UInt64,

    partyId       String,
    shopId        String,

    email         String,

    amount        UInt64,
    currency      String,

    providerName  String,
    status        Enum8('pending' = 1, 'processed' = 2, 'captured' = 3, 'cancelled' = 4, 'failed' = 5),
    errorReason   String,

    invoiceId     String,
    paymentId     String,
    sequenceId    UInt64,

    ip            String,
    bin           String,
    maskedPan     String,
    paymentTool   Enum8('bank_card' = 1, 'payment_terminal' = 2, 'digital_wallet' = 3),

    sign          Int8

) ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, paymentTool, status, currency, providerName, invoiceId, paymentId, sequenceId);