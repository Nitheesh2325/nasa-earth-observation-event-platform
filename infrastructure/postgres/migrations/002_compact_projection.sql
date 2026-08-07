BEGIN;

CREATE TABLE IF NOT EXISTS serving.event_detail_compact
  (LIKE serving.event_detail INCLUDING ALL);

ALTER TABLE serving.event_detail_compact
  DROP COLUMN IF EXISTS event_payload;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'serving.event_detail_compact'::regclass
      AND conname = 'event_detail_compact_source_type_fk'
  ) THEN
    ALTER TABLE serving.event_detail_compact
      ADD CONSTRAINT event_detail_compact_source_type_fk
      FOREIGN KEY (source_type) REFERENCES reference.source_type(source_type);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'serving.event_detail_compact'::regclass
      AND conname = 'event_detail_compact_gold_run_fk'
  ) THEN
    ALTER TABLE serving.event_detail_compact
      ADD CONSTRAINT event_detail_compact_gold_run_fk
      FOREIGN KEY (gold_run_id) REFERENCES load_control.gold_run(gold_run_id);
  END IF;
END
$$;

GRANT SELECT ON serving.event_detail_compact TO eo_api_readonly, eo_analyst_readonly;

COMMENT ON TABLE serving.event_detail_compact IS
  'A/B candidate: materialized serving projection without duplicated canonical JSON payload';

COMMIT;
