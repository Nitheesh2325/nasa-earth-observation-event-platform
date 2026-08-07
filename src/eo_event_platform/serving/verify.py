"""Verify the bounded PostgreSQL/PostGIS serving gate and emit compact JSON evidence."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from typing import Any

import psycopg


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


def timed_query(connection: psycopg.Connection[Any], sql: str, params: tuple[Any, ...] = (), repeats: int = 30) -> dict[str, float]:
    durations = []
    for _ in range(repeats):
        started = time.perf_counter()
        connection.execute(sql, params).fetchall()
        durations.append((time.perf_counter() - started) * 1000)
    return {
        "p50_ms": statistics.median(durations),
        "p95_ms": percentile(durations, 0.95),
        "p99_ms": percentile(durations, 0.99),
    }


def explain(connection: psycopg.Connection[Any], sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    row = connection.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params).fetchone()
    return row[0][0]


def expected_truth_counts(
    *, rows: int, unique_detections: int, original: int, replay: int, synthetic: int
) -> dict[str, int]:
    if min(rows, unique_detections, original, replay, synthetic) < 0:
        raise ValueError("expected truth counts cannot be negative")
    if original + replay + synthetic != rows:
        raise ValueError("expected source-type counts must reconcile to expected rows")
    return {
        "rows": rows,
        "unique_events": rows,
        "unique_detections": unique_detections,
        "original": original,
        "replay": replay,
        "synthetic": synthetic,
        "is_synthetic_true": synthetic,
    }


def verify(
    connection: psycopg.Connection[Any],
    expected_rows: int,
    *,
    expected_unique_detections: int,
    expected_original: int,
    expected_replay: int,
    expected_synthetic: int,
) -> dict[str, Any]:
    counts = connection.execute(
        """SELECT count(*), count(DISTINCT event_id), count(DISTINCT detection_id),
          count(*) FILTER (WHERE source_type='NASA_ORIGINAL'),
          count(*) FILTER (WHERE source_type='NASA_REPLAY'),
          count(*) FILTER (WHERE source_type='SYNTHETIC_SCALE_TEST'),
          count(*) FILTER (WHERE is_synthetic)
          FROM serving.event_detail"""
    ).fetchone()
    count_names = ("rows", "unique_events", "unique_detections", "original", "replay", "synthetic", "is_synthetic_true")
    count_result = dict(zip(count_names, counts, strict=True))
    expected_counts = expected_truth_counts(
        rows=expected_rows,
        unique_detections=expected_unique_detections,
        original=expected_original,
        replay=expected_replay,
        synthetic=expected_synthetic,
    )
    if count_result != expected_counts:
        raise RuntimeError(f"truth counts failed: {count_result}")

    spatial_invalid = connection.execute(
        """SELECT count(*) FROM serving.event_detail WHERE geometry IS NULL
           OR ST_SRID(geometry) <> 4326 OR ST_X(geometry) <> longitude
           OR ST_Y(geometry) <> latitude OR NOT ST_IsValid(geometry)"""
    ).fetchone()[0]
    aggregate_rows = connection.execute("SELECT coalesce(sum(event_message_count),0) FROM serving.dataset_daily_summary").fetchone()[0]
    lineage_rows = connection.execute("SELECT count(*) FROM serving.detection_lineage_summary").fetchone()[0]
    if spatial_invalid or aggregate_rows != expected_rows or lineage_rows != expected_unique_detections:
        raise RuntimeError("spatial or aggregate reconciliation failed")

    role_checks = {
        "api_can_select": connection.execute("SELECT has_table_privilege('eo_api_readonly','serving.event_detail','SELECT')").fetchone()[0],
        "api_can_insert": connection.execute("SELECT has_table_privilege('eo_api_readonly','serving.event_detail','INSERT')").fetchone()[0],
        "monitor_can_read_loads": connection.execute("SELECT has_table_privilege('eo_monitoring','load_control.database_load_run','SELECT')").fetchone()[0],
        "public_can_connect": connection.execute("SELECT has_database_privilege('public','eo_intelligence','CONNECT')").fetchone()[0],
    }
    if role_checks != {"api_can_select": True, "api_can_insert": False, "monitor_can_read_loads": True, "public_can_connect": False}:
        raise RuntimeError(f"role checks failed: {role_checks}")

    with connection.transaction(force_rollback=True):
        connection.execute("CREATE TEMP TABLE conflict_probe (payload jsonb NOT NULL)")
        connection.execute(
            """INSERT INTO conflict_probe SELECT jsonb_set(event_payload, '{governed_content_hash}', '"conflict"')
               FROM serving.event_detail ORDER BY event_id LIMIT 1"""
        )
        conflict_count = connection.execute(
            """SELECT count(*) FROM conflict_probe s JOIN serving.event_detail e
               ON e.event_id=s.payload->>'event_id'
               WHERE e.governed_content_hash <> s.payload->>'governed_content_hash'"""
        ).fetchone()[0]
        if conflict_count != 1:
            raise RuntimeError("content-conflict guard did not detect the probe")
    post_probe_rows = connection.execute("SELECT count(*) FROM serving.event_detail").fetchone()[0]
    if post_probe_rows != expected_rows:
        raise RuntimeError("conflict probe changed serving data")

    sample_lineage = connection.execute("SELECT lineage_root_id FROM serving.event_detail ORDER BY event_id LIMIT 1").fetchone()[0]
    queries = {
        "summary": ("SELECT source_type, count(*) FROM serving.event_detail GROUP BY source_type", ()),
        "spatial_bbox": ("SELECT count(*) FROM serving.event_detail WHERE geometry && ST_MakeEnvelope(-20,-20,20,20,4326)", ()),
        "lineage": ("SELECT event_id,event_timestamp FROM serving.event_detail WHERE lineage_root_id=%s ORDER BY scheduled_replay_timestamp NULLS LAST", (sample_lineage,)),
        "daily_aggregate": ("SELECT * FROM serving.dataset_daily_summary WHERE event_date=%s", ("2026-04-01",)),
    }
    latency = {name: timed_query(connection, sql, params) for name, (sql, params) in queries.items()}
    plans = {name: explain(connection, sql, params) for name, (sql, params) in queries.items()}

    sizes = connection.execute(
        """SELECT pg_database_size(current_database()),
          pg_total_relation_size('serving.event_detail'),
          pg_relation_size('serving.event_detail'),
          pg_indexes_size('serving.event_detail')"""
    ).fetchone()
    load_status = connection.execute(
        "SELECT status, count(*) FROM load_control.database_load_run GROUP BY status ORDER BY status"
    ).fetchall()
    return {
        "counts": count_result,
        "spatial_invalid": spatial_invalid,
        "dataset_daily_sum": aggregate_rows,
        "lineage_summary_rows": lineage_rows,
        "role_checks": role_checks,
        "content_conflicts_detected": conflict_count,
        "post_conflict_probe_rows": post_probe_rows,
        "sizes_bytes": {"database": sizes[0], "event_detail_total": sizes[1], "event_detail_heap": sizes[2], "event_detail_indexes": sizes[3]},
        "load_status": {row[0]: row[1] for row in load_status},
        "latency": latency,
        "plans": plans,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--expected-unique-detections", type=int, required=True)
    parser.add_argument("--expected-original", type=int, required=True)
    parser.add_argument("--expected-replay", type=int, required=True)
    parser.add_argument("--expected-synthetic", type=int, required=True)
    args = parser.parse_args()
    with psycopg.connect(args.dsn, autocommit=True) as connection:
        result = verify(
            connection,
            args.expected_rows,
            expected_unique_detections=args.expected_unique_detections,
            expected_original=args.expected_original,
            expected_replay=args.expected_replay,
            expected_synthetic=args.expected_synthetic,
        )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
