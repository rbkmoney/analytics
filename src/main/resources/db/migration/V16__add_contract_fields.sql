CREATE TYPE analytics.contract_status AS ENUM ('active', 'terminated', 'expired');

ALTER TABLE analytics.contract
    ADD COLUMN created_at TIMESTAMP WITHOUT TIME ZONE,
    ADD COLUMN valid_since TIMESTAMP WITHOUT TIME ZONE,
    ADD COLUMN valid_until TIMESTAMP WITHOUT TIME ZONE,
    ADD COLUMN status analytics.contract_status,
    ADD COLUMN status_terminated_at TIMESTAMP WITHOUT TIME ZONE,
    ADD COLUMN legal_agreement_signed_at TIMESTAMP WITHOUT TIME ZONE,
    ADD COLUMN legal_agreement_id CHARACTER VARYING,
    ADD COLUMN legal_agreement_valid_until TIMESTAMP WITHOUT TIME ZONE;


