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
    paymentTool   String,

    sign          Int8

) ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, paymentTool, status, currency, providerName, invoiceId, paymentId, sequenceId);

DROP TABLE IF EXISTS analytic.events_sink_refund;

create table analytic.events_sink_refund
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
    reason        String,

    status        Enum8('pending' = 1, 'succeeded' = 2, 'failed' = 3),
    errorReason   String,

    invoiceId     String,
    refundId      String,
    paymentId     String,
    sequenceId    UInt64,

    ip            String,

    sign          Int8
) ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMM (timestamp)
ORDER BY (eventTimeHour, partyId, shopId, status, currency, providerName, invoiceId, refundId, paymentId, sequenceId);