-- Phase 8A runtime login. A password is assigned out-of-band and never stored here.

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'eo_api_runtime') THEN
    CREATE ROLE eo_api_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
  END IF;
END
$$;

GRANT eo_api_readonly TO eo_api_runtime;
GRANT CONNECT ON DATABASE eo_intelligence TO eo_api_runtime;
REVOKE CREATE, TEMPORARY ON DATABASE eo_intelligence FROM eo_api_runtime;
ALTER ROLE eo_api_runtime SET default_transaction_read_only = on;
ALTER ROLE eo_api_runtime SET statement_timeout = '15s';

GRANT USAGE ON SCHEMA public TO eo_api_readonly;

CREATE INDEX IF NOT EXISTS event_detail_activity_time_idx
  ON serving.event_detail
  (COALESCE(scheduled_replay_timestamp, event_timestamp), event_id);

CREATE TABLE IF NOT EXISTS serving.dataset_activity_daily_summary (
  gold_run_id uuid NOT NULL REFERENCES load_control.gold_run(gold_run_id),
  activity_date date NOT NULL,
  source_dataset text NOT NULL,
  source_type text NOT NULL,
  event_message_count bigint NOT NULL,
  unique_event_count bigint NOT NULL,
  unique_detection_count bigint NOT NULL,
  original_message_count bigint NOT NULL,
  replay_message_count bigint NOT NULL,
  synthetic_message_count bigint NOT NULL,
  generated_at timestamptz NOT NULL,
  PRIMARY KEY (gold_run_id, activity_date, source_dataset, source_type)
);

INSERT INTO serving.dataset_activity_daily_summary
SELECT gold_run_id, COALESCE(scheduled_replay_timestamp,event_timestamp)::date,
  source_dataset, source_type, count(*), count(DISTINCT event_id),
  count(DISTINCT detection_id),
  count(*) FILTER (WHERE source_type='NASA_ORIGINAL'),
  count(*) FILTER (WHERE source_type='NASA_REPLAY'),
  count(*) FILTER (WHERE source_type='SYNTHETIC_SCALE_TEST'), clock_timestamp()
FROM serving.event_detail
GROUP BY gold_run_id, COALESCE(scheduled_replay_timestamp,event_timestamp)::date,
  source_dataset, source_type
ON CONFLICT (gold_run_id, activity_date, source_dataset, source_type)
DO UPDATE SET event_message_count=excluded.event_message_count,
  unique_event_count=excluded.unique_event_count,
  unique_detection_count=excluded.unique_detection_count,
  original_message_count=excluded.original_message_count,
  replay_message_count=excluded.replay_message_count,
  synthetic_message_count=excluded.synthetic_message_count,
  generated_at=excluded.generated_at;

GRANT SELECT ON serving.dataset_activity_daily_summary TO eo_api_readonly;

COMMENT ON ROLE eo_api_runtime IS
  'Phase 8A read-only API login; credentials are assigned outside migrations.';
