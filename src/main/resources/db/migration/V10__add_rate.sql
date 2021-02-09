CREATE TABLE analytics.rate (
  id                        BIGSERIAL                   NOT NULL,
  event_id                  BIGINT                      NOT NULL,
  event_time                TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  source_id                 CHARACTER VARYING           NOT NULL,
  lower_bound_inclusive     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  upper_bound_exclusive     TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  source_symbolic_code      CHARACTER VARYING           NOT NULL,
  source_exponent           SMALLINT                    NOT NULL,
  destination_symbolic_code CHARACTER VARYING           NOT NULL,
  destination_exponent      SMALLINT                    NOT NULL,
  exchange_rate_rational_p  BIGINT                      NOT NULL,
  exchange_rate_rational_q  BIGINT                      NOT NULL,
  CONSTRAINT rate_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX rate_source_id_idx
  ON analytics.rate (source_id, source_symbolic_code, destination_symbolic_code);
