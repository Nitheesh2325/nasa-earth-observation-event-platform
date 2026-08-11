BEGIN;

CREATE OR REPLACE VIEW serving.platform_operational_status AS
WITH latest AS (
  SELECT load_run_id, gold_run_id, manifest_rows, staged_rows, inserted_rows,
    already_present_rows, completed_at
  FROM load_control.database_load_run
  WHERE status = 'SUCCEEDED'
  ORDER BY completed_at DESC, load_run_id DESC
  LIMIT 1
)
SELECT latest.gold_run_id AS latest_manifest_id,
  gold.manifest_sha256::text AS latest_manifest_sha256,
  gold.gold_contract_version AS latest_gold_version,
  gold.pipeline_version AS platform_version,
  latest.completed_at AS data_freshness,
  CASE WHEN latest.staged_rows = latest.manifest_rows
    AND coalesce(latest.inserted_rows, 0) + coalesce(latest.already_present_rows, 0) = latest.manifest_rows
    THEN 'PASSED' ELSE 'FAILED' END AS quality_gate_status
FROM latest
JOIN load_control.gold_run gold USING (gold_run_id);

GRANT SELECT ON serving.platform_operational_status TO eo_api_readonly;

COMMENT ON VIEW serving.platform_operational_status IS
  'Bounded safe operational projection for the read-only platform status API.';

COMMIT;
