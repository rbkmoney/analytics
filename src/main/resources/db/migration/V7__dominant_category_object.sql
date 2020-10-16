CREATE TABLE analytics.category
(
    id          BIGSERIAL         NOT NULL,
    version_id  BIGINT            NOT NULL,
    category_id INT               NOT NULL,
    name        CHARACTER VARYING NOT NULL,
    description CHARACTER VARYING NOT NULL,
    type        CHARACTER VARYING,
    deleted     BOOLEAN DEFAULT false,
    CONSTRAINT category_pkey PRIMARY KEY (id)
);

CREATE INDEX category_version_id on analytics.category (version_id);
CREATE INDEX category_idx on analytics.category (category_id);

CREATE TABLE analytics.dominant
(
    last_version BIGINT    NOT NULL,
    CONSTRAINT dominant_pkey PRIMARY KEY (last_version)
)
