TRUNCATE TABLE analytics.contract;
TRUNCATE TABLE analytics.contractor;

DROP INDEX IF EXISTS analytics.contract_uidx;
CREATE UNIQUE INDEX contract_uidx ON analytics.contract (party_id, contract_id);

DROP INDEX IF EXISTS analytics.contractor_id_idx;
CREATE UNIQUE INDEX contractor_id_idx ON analytics.contractor (party_id, contractor_id);
