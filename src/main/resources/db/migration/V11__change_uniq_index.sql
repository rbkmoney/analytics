TRUNCATE TABLE analytics.rate;

DROP INDEX IF EXISTS analytics.rate_source_id_idx;

CREATE UNIQUE INDEX rate_source_id_idx
  ON analytics.rate (source_id, source_symbolic_code, destination_symbolic_code, lower_bound_inclusive, upper_bound_exclusive);
