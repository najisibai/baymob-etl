CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS sf311 (
  request_id        VARCHAR PRIMARY KEY,
  created_at        TIMESTAMPTZ,
  closed_at         TIMESTAMPTZ,
  status            TEXT,
  category          TEXT,
  subcategory       TEXT,
  neighborhood      TEXT,
  latitude          DOUBLE PRECISION,
  longitude         DOUBLE PRECISION,
  raw               JSONB,
  created_at_ingest TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sf311_created_at ON sf311(created_at);
CREATE INDEX IF NOT EXISTS idx_sf311_neighborhood ON sf311(neighborhood);


CREATE TABLE IF NOT EXISTS bart_ridership_daily (
  date              DATE PRIMARY KEY,
  entries           BIGINT,
  exits             BIGINT,
  created_at_ingest TIMESTAMPTZ DEFAULT NOW()
);