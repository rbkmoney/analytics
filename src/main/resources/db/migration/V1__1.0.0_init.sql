CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.balance_change
(
    id        BIGSERIAL                   NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    party_id  CHARACTER VARYING           NOT NULL,
    shop_id   CHARACTER VARYING           NOT NULL,
    amount    BIGINT                      NOT NULL,
    currency  CHARACTER VARYING           NOT NULL,
    CONSTRAINT balance_change_pkey PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS balance_change_timestamp on analytics.balance_change (timestamp);
CREATE INDEX IF NOT EXISTS balance_change_party_id on analytics.balance_change (party_id);
CREATE INDEX IF NOT EXISTS balance_change_shop_id on analytics.balance_change (shop_id);
