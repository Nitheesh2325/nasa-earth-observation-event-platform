BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS serving;
CREATE SCHEMA IF NOT EXISTS load_control;
CREATE SCHEMA IF NOT EXISTS quality;

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON DATABASE eo_intelligence FROM PUBLIC;

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'eo_loader') THEN
    CREATE ROLE eo_loader NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'eo_api_readonly') THEN
    CREATE ROLE eo_api_readonly NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'eo_analyst_readonly') THEN
    CREATE ROLE eo_analyst_readonly NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'eo_monitoring') THEN
    CREATE ROLE eo_monitoring NOLOGIN;
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS reference.source_type (
  source_type text PRIMARY KEY,
  is_synthetic boolean NOT NULL,
  description text NOT NULL
);

INSERT INTO reference.source_type VALUES
  ('NASA_ORIGINAL', false, 'Original NASA detection'),
  ('NASA_REPLAY', false, 'Controlled replay of an original NASA detection'),
  ('SYNTHETIC_SCALE_TEST', true, 'Explicitly synthetic scale-test event')
ON CONFLICT (source_type) DO NOTHING;

CREATE TABLE IF NOT EXISTS load_control.gold_run (
  gold_run_id uuid PRIMARY KEY,
  gold_contract_version text NOT NULL,
  pipeline_version text NOT NULL,
  source_silver_path text NOT NULL,
  manifest_sha256 char(64) NOT NULL,
  expected_rows bigint NOT NULL CHECK (expected_rows >= 0),
  created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS load_control.database_load_run (
  load_run_id uuid PRIMARY KEY,
  gold_run_id uuid NOT NULL REFERENCES load_control.gold_run(gold_run_id),
  idempotency_key char(64) NOT NULL UNIQUE,
  status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
  manifest_rows bigint NOT NULL CHECK (manifest_rows >= 0),
  staged_rows bigint,
  inserted_rows bigint,
  already_present_rows bigint,
  started_at timestamptz NOT NULL,
  completed_at timestamptz,
  duration_seconds double precision,
  error_summary text
);

CREATE TABLE IF NOT EXISTS load_control.loaded_artifact (
  load_run_id uuid NOT NULL REFERENCES load_control.database_load_run(load_run_id),
  artifact_path text NOT NULL,
  artifact_sha256 char(64) NOT NULL,
  artifact_bytes bigint NOT NULL CHECK (artifact_bytes >= 0),
  expected_rows bigint NOT NULL CHECK (expected_rows >= 0),
  PRIMARY KEY (load_run_id, artifact_path)
);

CREATE TABLE IF NOT EXISTS serving.event_detail (
  event_id text PRIMARY KEY,
  detection_id text NOT NULL,
  lineage_root_id text NOT NULL,
  source_type text NOT NULL REFERENCES reference.source_type(source_type),
  source_dataset text NOT NULL,
  source_record_id text NOT NULL,
  is_synthetic boolean NOT NULL,
  ingestion_run_id text NOT NULL,
  replay_run_id text,
  synthetic_generation_id text,
  parent_event_id text,
  scheduled_replay_timestamp timestamptz,
  replay_iteration bigint,
  replay_sequence_number bigint,
  event_timestamp timestamptz NOT NULL,
  ingestion_timestamp timestamptz NOT NULL,
  processing_timestamp timestamptz NOT NULL,
  latitude double precision NOT NULL CHECK (latitude BETWEEN -90 AND 90),
  longitude double precision NOT NULL CHECK (longitude BETWEEN -180 AND 180),
  geometry geometry(Point,4326) NOT NULL,
  bright_ti4_kelvin double precision,
  bright_ti5_kelvin double precision,
  fire_radiative_power_mw double precision,
  scan_km double precision,
  track_km double precision,
  confidence text,
  day_night text CHECK (day_night IN ('D', 'N')),
  satellite text,
  instrument text,
  source_product_version text,
  schema_version text NOT NULL CHECK (schema_version = '1.0.0'),
  pipeline_version text NOT NULL,
  raw_object_uri text,
  raw_file_name text,
  raw_row_number bigint,
  raw_payload_hash text,
  kafka_topic text,
  kafka_partition bigint CHECK (kafka_partition IS NULL OR kafka_partition >= 0),
  kafka_offset bigint CHECK (kafka_offset IS NULL OR kafka_offset >= 0),
  kafka_timestamp timestamptz,
  validation_status text NOT NULL CHECK (validation_status = 'ACCEPTED'),
  deduplication_status text NOT NULL CHECK (deduplication_status = 'UNIQUE'),
  enrichment_status text,
  spark_processing_run_id text NOT NULL,
  gold_run_id uuid NOT NULL REFERENCES load_control.gold_run(gold_run_id),
  governed_content_hash char(64) NOT NULL,
  event_payload jsonb NOT NULL,
  loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CHECK (ST_SRID(geometry) = 4326),
  CHECK (ST_X(geometry) = longitude AND ST_Y(geometry) = latitude),
  CHECK (event_timestamp <= ingestion_timestamp),
  CHECK (
    (source_type = 'NASA_ORIGINAL' AND NOT is_synthetic AND replay_run_id IS NULL AND synthetic_generation_id IS NULL)
    OR (source_type = 'NASA_REPLAY' AND NOT is_synthetic AND replay_run_id IS NOT NULL AND scheduled_replay_timestamp IS NOT NULL AND replay_iteration > 0 AND replay_sequence_number >= 0)
    OR (source_type = 'SYNTHETIC_SCALE_TEST' AND is_synthetic AND synthetic_generation_id IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS event_detail_detection_time_idx ON serving.event_detail (detection_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS event_detail_lineage_replay_idx ON serving.event_detail (lineage_root_id, scheduled_replay_timestamp);
CREATE INDEX IF NOT EXISTS event_detail_geometry_gist_idx ON serving.event_detail USING gist (geometry);
CREATE INDEX IF NOT EXISTS event_detail_source_time_idx ON serving.event_detail (source_type, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS event_detail_event_time_brin_idx ON serving.event_detail USING brin (event_timestamp);

CREATE TABLE IF NOT EXISTS serving.dataset_daily_summary (
  gold_run_id uuid NOT NULL REFERENCES load_control.gold_run(gold_run_id),
  event_date date NOT NULL,
  source_dataset text NOT NULL,
  source_type text NOT NULL,
  event_message_count bigint NOT NULL,
  unique_event_count bigint NOT NULL,
  unique_detection_count bigint NOT NULL,
  original_message_count bigint NOT NULL,
  replay_message_count bigint NOT NULL,
  synthetic_message_count bigint NOT NULL,
  generated_at timestamptz NOT NULL,
  PRIMARY KEY (gold_run_id, event_date, source_dataset, source_type)
);

CREATE TABLE IF NOT EXISTS serving.detection_lineage_summary (
  gold_run_id uuid NOT NULL REFERENCES load_control.gold_run(gold_run_id),
  lineage_root_id text NOT NULL,
  event_message_count bigint NOT NULL,
  original_message_count bigint NOT NULL,
  replay_message_count bigint NOT NULL,
  synthetic_message_count bigint NOT NULL,
  first_event_timestamp timestamptz NOT NULL,
  last_activity_timestamp timestamptz NOT NULL,
  generated_at timestamptz NOT NULL,
  PRIMARY KEY (gold_run_id, lineage_root_id)
);

CREATE TABLE IF NOT EXISTS quality.load_quality_metric (
  load_run_id uuid NOT NULL REFERENCES load_control.database_load_run(load_run_id),
  metric_name text NOT NULL,
  metric_value numeric NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (load_run_id, metric_name)
);

GRANT USAGE ON SCHEMA serving TO eo_api_readonly, eo_analyst_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO eo_api_readonly, eo_analyst_readonly;
GRANT USAGE ON SCHEMA load_control, quality TO eo_monitoring;
GRANT SELECT ON ALL TABLES IN SCHEMA load_control, quality TO eo_monitoring;

COMMIT;
