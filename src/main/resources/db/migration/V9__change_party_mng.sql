DROP TABLE IF EXISTS analytics.party;
DROP TABLE IF EXISTS analytics.shop;

DROP TYPE IF EXISTS analytics.contractor;

CREATE TYPE analytics.contractor_type AS ENUM ('registered_user', 'legal_entity', 'private_entity');

CREATE TABLE analytics.party
(
    id                                            BIGSERIAL                   NOT NULL,
    event_id                                      BIGINT                      NOT NULL,
    event_time                                    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    party_id                                      CHARACTER VARYING           NOT NULL,
    created_at                                    TIMESTAMP WITHOUT TIME ZONE NOT NULL NOT NULL,
    email                                         CHARACTER VARYING,
    blocking                                      analytics.blocking          NOT NULL,
    blocked_reason                                CHARACTER VARYING,
    blocked_since                                 TIMESTAMP WITHOUT TIME ZONE,
    unblocked_reason                              CHARACTER VARYING,
    unblocked_since                               TIMESTAMP WITHOUT TIME ZONE,
    suspension                                    analytics.suspension        NOT NULL,
    suspension_active_since                       TIMESTAMP WITHOUT TIME ZONE,
    suspension_suspended_since                    TIMESTAMP WITHOUT TIME ZONE,
    revision_id                                   CHARACTER VARYING,
    revision_changed_at                           TIMESTAMP WITHOUT TIME ZONE,

    CONSTRAINT party_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX party_party_id_uidx ON analytics.party (party_id);
CREATE INDEX party_created_at_idx ON analytics.party (created_at);

CREATE TABLE analytics.shop
(
    id                         BIGSERIAL                   NOT NULL,
    event_id                   BIGINT                      NOT NULL,
    event_time                 TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    party_id                   CHARACTER VARYING           NOT NULL,
    shop_id                    CHARACTER VARYING           NOT NULL,

    contract_id                CHARACTER VARYING           NOT NULL,
    category_id                INT,
    payout_tool_id             CHARACTER VARYING,
    payout_schedule_id         INT,
    created_at                 TIMESTAMP WITHOUT TIME ZONE,
    blocking                   analytics.blocking,
    blocked_reason             CHARACTER VARYING,
    blocked_since              TIMESTAMP WITHOUT TIME ZONE,
    unblocked_reason           CHARACTER VARYING,
    unblocked_since            TIMESTAMP WITHOUT TIME ZONE,
    suspension                 analytics.suspension,
    suspension_active_since    TIMESTAMP WITHOUT TIME ZONE,
    suspension_suspended_since TIMESTAMP WITHOUT TIME ZONE,
    details_name               CHARACTER VARYING,
    details_description        CHARACTER VARYING,
    location_url               CHARACTER VARYING,
    account_currency_code      CHARACTER VARYING,
    account_settlement         CHARACTER VARYING,
    account_guarantee          CHARACTER VARYING,
    account_payout             CHARACTER VARYING,

    contractor_id                                 CHARACTER VARYING,
    contractor_type                               analytics.contractor_type,
    reg_user_email                                CHARACTER VARYING,
    legal_entity_type                             analytics.legal_entity,
    russian_legal_entity_name                     CHARACTER VARYING,
    russian_legal_entity_registered_number        CHARACTER VARYING,
    russian_legal_entity_inn                      CHARACTER VARYING,
    russian_legal_entity_actual_address           CHARACTER VARYING,
    russian_legal_entity_post_address             CHARACTER VARYING,
    russian_legal_entity_representative_position  CHARACTER VARYING,
    russian_legal_entity_representative_full_name CHARACTER VARYING,
    russian_legal_entity_representative_document  CHARACTER VARYING,
    russian_legal_entity_bank_account             CHARACTER VARYING,
    russian_legal_entity_bank_name                CHARACTER VARYING,
    russian_legal_entity_bank_post_account        CHARACTER VARYING,
    russian_legal_entity_bank_bik                 CHARACTER VARYING,

    international_legal_entity_name               CHARACTER VARYING,
    international_legal_entity_trading_name       CHARACTER VARYING,
    international_legal_entity_registered_address CHARACTER VARYING,
    international_legal_entity_registered_number  CHARACTER VARYING,
    international_actual_address                  CHARACTER VARYING,

    private_entity_type                           analytics.private_entity,
    russian_private_entity_first_name             CHARACTER VARYING,
    russian_private_entity_second_name            CHARACTER VARYING,
    russian_private_entity_middle_name            CHARACTER VARYING,
    russian_private_entity_phone_number           CHARACTER VARYING,
    russian_private_entity_email                  CHARACTER VARYING,

    contractor_identification_level               analytics.contractor_identification_lvl,
    CONSTRAINT shop_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX party_id_shop_id_uidx ON analytics.shop (party_id, shop_id);
CREATE INDEX shop_created_at_idx ON analytics.shop (created_at);

CREATE TABLE analytics.contract
(
    id                         BIGSERIAL                   NOT NULL,
    event_id                   BIGINT                      NOT NULL,
    event_time                 TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    party_id                   CHARACTER VARYING           NOT NULL,
    contract_id                CHARACTER VARYING           NOT NULL,
    contractor_id              CHARACTER VARYING           NOT NULL,

    CONSTRAINT contract_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX contract_uidx ON analytics.contract (contract_id);

CREATE TABLE analytics.contractor
(
    id                         BIGSERIAL                   NOT NULL,
    event_id                   BIGINT                      NOT NULL,
    event_time                 TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    party_id                   CHARACTER VARYING           NOT NULL,

    contractor_id                                 CHARACTER VARYING NOT NULL,
    contractor_type                               analytics.contractor_type,
    reg_user_email                                CHARACTER VARYING,
    legal_entity_type                             analytics.legal_entity,
    russian_legal_entity_name                     CHARACTER VARYING,
    russian_legal_entity_registered_number        CHARACTER VARYING,
    russian_legal_entity_inn                      CHARACTER VARYING,
    russian_legal_entity_actual_address           CHARACTER VARYING,
    russian_legal_entity_post_address             CHARACTER VARYING,
    russian_legal_entity_representative_position  CHARACTER VARYING,
    russian_legal_entity_representative_full_name CHARACTER VARYING,
    russian_legal_entity_representative_document  CHARACTER VARYING,
    russian_legal_entity_bank_account             CHARACTER VARYING,
    russian_legal_entity_bank_name                CHARACTER VARYING,
    russian_legal_entity_bank_post_account        CHARACTER VARYING,
    russian_legal_entity_bank_bik                 CHARACTER VARYING,

    international_legal_entity_name               CHARACTER VARYING,
    international_legal_entity_trading_name       CHARACTER VARYING,
    international_legal_entity_registered_address CHARACTER VARYING,
    international_legal_entity_registered_number  CHARACTER VARYING,
    international_actual_address                  CHARACTER VARYING,

    private_entity_type                           analytics.private_entity,
    russian_private_entity_first_name             CHARACTER VARYING,
    russian_private_entity_second_name            CHARACTER VARYING,
    russian_private_entity_middle_name            CHARACTER VARYING,
    russian_private_entity_phone_number           CHARACTER VARYING,
    russian_private_entity_email                  CHARACTER VARYING,

    contractor_identification_level               analytics.contractor_identification_lvl,

    CONSTRAINT contractor_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX contractor_id_idx ON analytics.contractor (contractor_id);
