"""Manifest-validated bulk loader for the PostgreSQL/PostGIS serving projection."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg


INSERT_EVENT_SQL = """
INSERT INTO serving.event_detail (
  event_id, detection_id, lineage_root_id, source_type, source_dataset,
  source_record_id, is_synthetic, ingestion_run_id, replay_run_id,
  synthetic_generation_id, parent_event_id, scheduled_replay_timestamp,
  replay_iteration, replay_sequence_number, event_timestamp,
  ingestion_timestamp, processing_timestamp, latitude, longitude, geometry,
  bright_ti4_kelvin, bright_ti5_kelvin, fire_radiative_power_mw, scan_km,
  track_km, confidence, day_night, satellite, instrument,
  source_product_version, schema_version, pipeline_version, raw_object_uri,
  raw_file_name, raw_row_number, raw_payload_hash, kafka_topic,
  kafka_partition, kafka_offset, kafka_timestamp, validation_status,
  deduplication_status, enrichment_status, spark_processing_run_id,
  gold_run_id, governed_content_hash
)
SELECT
  payload->>'event_id', payload->>'detection_id', payload->>'lineage_root_id',
  payload->>'source_type', payload->>'source_dataset', payload->>'source_record_id',
  (payload->>'is_synthetic')::boolean, payload->>'ingestion_run_id',
  NULLIF(payload->>'replay_run_id',''), NULLIF(payload->>'synthetic_generation_id',''),
  NULLIF(payload->>'parent_event_id',''),
  NULLIF(payload->>'scheduled_replay_timestamp','')::timestamptz,
  NULLIF(payload->>'replay_iteration','')::bigint,
  NULLIF(payload->>'replay_sequence_number','')::bigint,
  (payload->>'event_timestamp')::timestamptz,
  (payload->>'ingestion_timestamp')::timestamptz,
  (payload->>'processing_timestamp')::timestamptz,
  (payload->>'latitude')::double precision,
  (payload->>'longitude')::double precision,
  ST_SetSRID(ST_MakePoint(
    (payload->>'longitude')::double precision,
    (payload->>'latitude')::double precision
  ), 4326),
  NULLIF(payload->>'bright_ti4_kelvin','')::double precision,
  NULLIF(payload->>'bright_ti5_kelvin','')::double precision,
  NULLIF(payload->>'fire_radiative_power_mw','')::double precision,
  NULLIF(payload->>'scan_km','')::double precision,
  NULLIF(payload->>'track_km','')::double precision,
  NULLIF(payload->>'confidence',''), NULLIF(payload->>'day_night',''),
  NULLIF(payload->>'satellite',''), NULLIF(payload->>'instrument',''),
  NULLIF(payload->>'source_product_version',''), payload->>'schema_version',
  payload->>'pipeline_version', NULLIF(payload->>'raw_object_uri',''),
  NULLIF(payload->>'raw_file_name',''), NULLIF(payload->>'raw_row_number','')::bigint,
  NULLIF(payload->>'raw_payload_hash',''), NULLIF(payload->>'kafka_topic',''),
  NULLIF(payload->>'kafka_partition','')::bigint,
  NULLIF(payload->>'kafka_offset','')::bigint,
  NULLIF(payload->>'kafka_timestamp','')::timestamptz,
  payload->>'validation_status', payload->>'deduplication_status',
  NULLIF(payload->>'enrichment_status',''), payload->>'spark_processing_run_id',
  (payload->>'gold_run_id')::uuid, payload->>'governed_content_hash'
FROM temp_gold_event
ON CONFLICT (event_id) DO NOTHING
"""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(path: Path, *, expected_rows: int) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    manifest = json.loads(raw)
    required = {
        "gold_run_id", "gold_contract_version", "pipeline_version",
        "source_silver_path", "expected_rows", "load_artifact_rows", "artifacts",
    }
    missing = sorted(required - manifest.keys())
    if missing:
        raise ValueError(f"manifest missing fields: {missing}")
    if manifest["expected_rows"] != expected_rows or manifest["load_artifact_rows"] != expected_rows:
        raise ValueError("manifest row count does not match the bounded gate")
    for artifact in manifest["artifacts"]:
        artifact_path = Path(artifact["path"])
        if not artifact_path.is_absolute():
            artifact_path = path.parent / artifact_path
        if not artifact_path.is_file():
            raise FileNotFoundError(artifact_path)
        if artifact_path.stat().st_size != artifact["bytes"]:
            raise ValueError(f"artifact size mismatch: {artifact_path}")
        if sha256_file(artifact_path) != artifact["sha256"]:
            raise ValueError(f"artifact checksum mismatch: {artifact_path}")
    load_artifacts = [
        artifact for artifact in manifest["artifacts"]
        if artifact["path"].replace("\\", "/").startswith("load_artifact/part-")
        and artifact["path"].endswith(".json")
    ]
    if not load_artifacts:
        raise ValueError("manifest contains no JSON load artifacts")
    declared_rows = [artifact.get("rows") for artifact in load_artifacts]
    if any(value is not None for value in declared_rows):
        if any(not isinstance(value, int) or value < 0 for value in declared_rows):
            raise ValueError("load artifact row counts must be non-negative integers")
        if sum(declared_rows) != expected_rows:
            raise ValueError("load artifact row counts do not reconcile")
    manifest["_manifest_root"] = path.parent.as_posix()
    return manifest, hashlib.sha256(raw).hexdigest()


def iter_payloads(manifest: dict[str, Any]) -> Iterable[tuple[str]]:
    paths = sorted(
        Path(manifest["_manifest_root"]) / item["path"]
        for item in manifest["artifacts"]
        if item["path"].replace("\\", "/").startswith("load_artifact/part-")
        and item["path"].endswith(".json")
    )
    if not paths:
        raise ValueError("manifest contains no JSON load artifacts")
    for path in paths:
        with path.open("r", encoding="utf-8") as source:
            for line in source:
                if line.strip():
                    yield (line.strip(),)


def apply_migration(connection: psycopg.Connection[Any], migration_path: Path) -> None:
    connection.execute(migration_path.read_text(encoding="utf-8"))


def reconcile_idempotent_rows(persisted_rows: int, expected_rows: int) -> None:
    if persisted_rows != expected_rows:
        raise RuntimeError(
            f"successful load metadata exists but serving rows are {persisted_rows}, expected {expected_rows}"
        )


def load(connection: psycopg.Connection[Any], manifest_path: Path, expected_rows: int) -> dict[str, Any]:
    manifest, idempotency_key = load_manifest(manifest_path, expected_rows=expected_rows)
    existing = connection.execute(
        """SELECT load_run_id, inserted_rows, gold_run_id
           FROM load_control.database_load_run
           WHERE idempotency_key = %s AND status = 'SUCCEEDED'""",
        (idempotency_key,),
    ).fetchone()
    if existing:
        persisted_rows = connection.execute(
            "SELECT count(*) FROM serving.event_detail WHERE gold_run_id = %s",
            (existing[2],),
        ).fetchone()[0]
        reconcile_idempotent_rows(persisted_rows, expected_rows)
        return {"load_run_id": str(existing[0]), "inserted_rows": 0, "already_present_rows": expected_rows, "idempotent_noop": True}

    started = datetime.now(timezone.utc)
    clock = time.perf_counter()
    load_run_id = str(uuid.uuid4())
    manifest_sha = sha256_file(manifest_path)
    connection.execute(
        """INSERT INTO load_control.gold_run
           (gold_run_id, gold_contract_version, pipeline_version, source_silver_path, manifest_sha256, expected_rows, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (gold_run_id) DO NOTHING""",
        (manifest["gold_run_id"], manifest["gold_contract_version"], manifest["pipeline_version"], manifest["source_silver_path"], manifest_sha, expected_rows, manifest["completed_at"]),
    )
    connection.execute(
        """INSERT INTO load_control.database_load_run
           (load_run_id, gold_run_id, idempotency_key, status, manifest_rows, started_at)
           VALUES (%s,%s,%s,'RUNNING',%s,%s)""",
        (load_run_id, manifest["gold_run_id"], idempotency_key, expected_rows, started),
    )
    load_artifact_count = sum(
        item["path"].replace("\\", "/").startswith("load_artifact/part-")
        and item["path"].endswith(".json")
        for item in manifest["artifacts"]
    )
    for artifact in manifest["artifacts"]:
        is_load_artifact = artifact["path"].replace("\\", "/").startswith("load_artifact/part-") and artifact["path"].endswith(".json")
        artifact_rows = artifact.get("rows")
        if artifact_rows is None:
            artifact_rows = expected_rows if is_load_artifact and load_artifact_count == 1 else 0
        connection.execute(
            "INSERT INTO load_control.loaded_artifact VALUES (%s,%s,%s,%s,%s)",
            (load_run_id, artifact["path"], artifact["sha256"], artifact["bytes"], artifact_rows),
        )
    connection.commit()

    try:
        with connection.transaction():
            connection.execute("CREATE TEMP TABLE temp_gold_event (payload jsonb NOT NULL) ON COMMIT DROP")
            staged_rows = 0
            with connection.cursor().copy("COPY temp_gold_event (payload) FROM STDIN") as copy:
                for payload in iter_payloads(manifest):
                    copy.write_row(payload)
                    staged_rows += 1
            if staged_rows != expected_rows:
                raise RuntimeError(f"staged {staged_rows}, expected {expected_rows}")
            conflicts = connection.execute(
                """SELECT count(*) FROM temp_gold_event s JOIN serving.event_detail e
                   ON e.event_id = s.payload->>'event_id'
                   WHERE e.governed_content_hash <> s.payload->>'governed_content_hash'"""
            ).fetchone()[0]
            if conflicts:
                raise RuntimeError(f"{conflicts} event_id content conflicts")
            before = connection.execute("SELECT count(*) FROM serving.event_detail").fetchone()[0]
            inserted = connection.execute(INSERT_EVENT_SQL).rowcount
            after = connection.execute("SELECT count(*) FROM serving.event_detail").fetchone()[0]
            if after != before + inserted:
                raise RuntimeError("post-load row count does not reconcile")
            already_present = staged_rows - inserted
            connection.execute("DELETE FROM serving.dataset_daily_summary WHERE gold_run_id = %s", (manifest["gold_run_id"],))
            connection.execute(
                """INSERT INTO serving.dataset_daily_summary
                   SELECT %s, event_timestamp::date, source_dataset, source_type,
                     count(*), count(DISTINCT event_id), count(DISTINCT detection_id),
                     count(*) FILTER (WHERE source_type='NASA_ORIGINAL'),
                     count(*) FILTER (WHERE source_type='NASA_REPLAY'),
                     count(*) FILTER (WHERE source_type='SYNTHETIC_SCALE_TEST'), clock_timestamp()
                   FROM serving.event_detail WHERE gold_run_id=%s
                   GROUP BY event_timestamp::date, source_dataset, source_type""",
                (manifest["gold_run_id"], manifest["gold_run_id"]),
            )
            connection.execute("DELETE FROM serving.detection_lineage_summary WHERE gold_run_id = %s", (manifest["gold_run_id"],))
            connection.execute(
                """INSERT INTO serving.detection_lineage_summary
                   SELECT %s, lineage_root_id, count(*),
                     count(*) FILTER (WHERE source_type='NASA_ORIGINAL'),
                     count(*) FILTER (WHERE source_type='NASA_REPLAY'),
                     count(*) FILTER (WHERE source_type='SYNTHETIC_SCALE_TEST'),
                     min(event_timestamp), max(COALESCE(scheduled_replay_timestamp,event_timestamp)), clock_timestamp()
                   FROM serving.event_detail WHERE gold_run_id=%s GROUP BY lineage_root_id""",
                (manifest["gold_run_id"], manifest["gold_run_id"]),
            )
            connection.execute("ANALYZE serving.event_detail")
            connection.execute("ANALYZE serving.dataset_daily_summary")
            connection.execute("ANALYZE serving.detection_lineage_summary")
            completed = datetime.now(timezone.utc)
            duration = time.perf_counter() - clock
            connection.execute(
                """UPDATE load_control.database_load_run SET status='SUCCEEDED', staged_rows=%s,
                   inserted_rows=%s, already_present_rows=%s, completed_at=%s, duration_seconds=%s
                   WHERE load_run_id=%s""",
                (staged_rows, inserted, already_present, completed, duration, load_run_id),
            )
            for name, value in (("manifest_rows", expected_rows), ("staged_rows", staged_rows), ("inserted_rows", inserted), ("already_present_rows", already_present)):
                connection.execute("INSERT INTO quality.load_quality_metric (load_run_id, metric_name, metric_value) VALUES (%s,%s,%s)", (load_run_id, name, value))
        return {"load_run_id": load_run_id, "staged_rows": staged_rows, "inserted_rows": inserted, "already_present_rows": already_present, "duration_seconds": duration, "idempotent_noop": False}
    except Exception as error:
        connection.rollback()
        connection.execute(
            "UPDATE load_control.database_load_run SET status='FAILED', completed_at=clock_timestamp(), error_summary=%s WHERE load_run_id=%s",
            (type(error).__name__, load_run_id),
        )
        connection.commit()
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--migration",
        type=Path,
        action="append",
        default=[],
        help="Migration to apply before loading; repeat in execution order.",
    )
    parser.add_argument("--expected-rows", type=int, required=True)
    args = parser.parse_args()
    if args.expected_rows <= 0:
        raise ValueError("expected rows must be positive")
    with psycopg.connect(args.dsn, autocommit=False) as connection:
        for migration in args.migration:
            apply_migration(connection, migration)
        result = load(connection, args.manifest, args.expected_rows)
        connection.commit()
    for key, value in result.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
