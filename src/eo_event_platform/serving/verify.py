"""Verify the bounded PostgreSQL/PostGIS serving gate and emit compact JSON evidence."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
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
    expected_replay_factor: int,
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

    replay_values = connection.execute(
        """SELECT count(DISTINCT replay_sequence_number), min(replay_sequence_number),
          max(replay_sequence_number), count(DISTINCT replay_iteration),
          min(replay_iteration), max(replay_iteration),
          count(*) FILTER (WHERE parent_event_id IS NULL)
          FROM serving.event_detail WHERE source_type='NASA_REPLAY'"""
    ).fetchone()
    frequency_values = connection.execute(
        """SELECT coalesce(min(events_per_detection),0), coalesce(max(events_per_detection),0)
           FROM (SELECT count(*) AS events_per_detection FROM serving.event_detail
                 WHERE source_type='NASA_REPLAY' GROUP BY detection_id) frequencies"""
    ).fetchone()
    replay_result = {
        "unique_sequences": replay_values[0],
        "min_sequence": replay_values[1],
        "max_sequence": replay_values[2],
        "unique_iterations": replay_values[3],
        "min_iteration": replay_values[4],
        "max_iteration": replay_values[5],
        "null_parents": replay_values[6],
        "min_events_per_detection": frequency_values[0],
        "max_events_per_detection": frequency_values[1],
    }
    expected_replay_result = {
        "unique_sequences": expected_replay,
        "min_sequence": 0 if expected_replay else None,
        "max_sequence": expected_replay - 1 if expected_replay else None,
        "unique_iterations": expected_replay_factor if expected_replay else 0,
        "min_iteration": 1 if expected_replay else None,
        "max_iteration": expected_replay_factor if expected_replay else None,
        "null_parents": 0,
        "min_events_per_detection": expected_replay_factor if expected_replay else 0,
        "max_events_per_detection": expected_replay_factor if expected_replay else 0,
    }
    if replay_result != expected_replay_result:
        raise RuntimeError(f"replay reconciliation failed: {replay_result}")

    spatial_invalid = connection.execute(
        """SELECT count(*) FROM serving.event_detail WHERE geometry IS NULL
           OR ST_SRID(geometry) <> 4326 OR ST_X(geometry) <> longitude
           OR ST_Y(geometry) <> latitude OR NOT ST_IsValid(geometry)"""
    ).fetchone()[0]
    aggregate_values = connection.execute(
        """SELECT coalesce(sum(event_message_count),0), coalesce(sum(unique_event_count),0),
          coalesce(sum(original_message_count),0), coalesce(sum(replay_message_count),0),
          coalesce(sum(synthetic_message_count),0) FROM serving.dataset_daily_summary"""
    ).fetchone()
    lineage_values = connection.execute(
        """SELECT count(*), coalesce(sum(event_message_count),0),
          coalesce(min(event_message_count),0), coalesce(max(event_message_count),0)
          FROM serving.detection_lineage_summary"""
    ).fetchone()
    aggregate_result = {
        "event_messages": aggregate_values[0],
        "unique_events": aggregate_values[1],
        "original_messages": aggregate_values[2],
        "replay_messages": aggregate_values[3],
        "synthetic_messages": aggregate_values[4],
    }
    expected_aggregate_result = {
        "event_messages": expected_rows,
        "unique_events": expected_rows,
        "original_messages": expected_original,
        "replay_messages": expected_replay,
        "synthetic_messages": expected_synthetic,
    }
    lineage_result = {
        "rows": lineage_values[0],
        "event_messages": lineage_values[1],
        "min_events_per_lineage": lineage_values[2],
        "max_events_per_lineage": lineage_values[3],
    }
    expected_lineage_result = {
        "rows": expected_unique_detections,
        "event_messages": expected_rows,
        "min_events_per_lineage": expected_replay_factor,
        "max_events_per_lineage": expected_replay_factor,
    }
    if spatial_invalid or aggregate_result != expected_aggregate_result or lineage_result != expected_lineage_result:
        raise RuntimeError("spatial or aggregate reconciliation failed")

    load_values = connection.execute(
        """SELECT staged_rows, inserted_rows, already_present_rows, manifest_rows,
          duration_seconds, gold_run_id FROM load_control.database_load_run
          WHERE status='SUCCEEDED' ORDER BY completed_at DESC LIMIT 1"""
    ).fetchone()
    if not load_values:
        raise RuntimeError("successful load-control row is missing")
    loaded_artifact_rows = connection.execute(
        "SELECT coalesce(sum(expected_rows),0) FROM load_control.loaded_artifact WHERE load_run_id = "
        "(SELECT load_run_id FROM load_control.database_load_run WHERE status='SUCCEEDED' ORDER BY completed_at DESC LIMIT 1)"
    ).fetchone()[0]
    load_result = {
        "staged_rows": load_values[0],
        "inserted_rows": load_values[1],
        "already_present_rows": load_values[2],
        "manifest_rows": load_values[3],
        "duration_seconds": load_values[4],
        "gold_run_id": str(load_values[5]),
        "loaded_artifact_rows": loaded_artifact_rows,
    }
    if tuple(load_values[:4]) != (expected_rows, expected_rows, 0, expected_rows) or loaded_artifact_rows != expected_rows:
        raise RuntimeError(f"load-control reconciliation failed: {load_result}")

    role_checks = {
        "api_can_select": connection.execute("SELECT has_table_privilege('eo_api_readonly','serving.event_detail','SELECT')").fetchone()[0],
        "api_can_insert": connection.execute("SELECT has_table_privilege('eo_api_readonly','serving.event_detail','INSERT')").fetchone()[0],
        "monitor_can_read_loads": connection.execute("SELECT has_table_privilege('eo_monitoring','load_control.database_load_run','SELECT')").fetchone()[0],
        "public_can_connect": connection.execute("SELECT has_database_privilege('public','eo_intelligence','CONNECT')").fetchone()[0],
    }
    if role_checks != {"api_can_select": True, "api_can_insert": False, "monitor_can_read_loads": True, "public_can_connect": False}:
        raise RuntimeError(f"role checks failed: {role_checks}")

    with connection.transaction(force_rollback=True):
        connection.execute("CREATE TEMP TABLE conflict_probe (event_id text NOT NULL, governed_content_hash text NOT NULL)")
        connection.execute(
            """INSERT INTO conflict_probe
               SELECT event_id, 'conflict' FROM serving.event_detail ORDER BY event_id LIMIT 1"""
        )
        conflict_count = connection.execute(
            """SELECT count(*) FROM conflict_probe s JOIN serving.event_detail e
               ON e.event_id=s.event_id
               WHERE e.governed_content_hash <> s.governed_content_hash"""
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
        "replay": replay_result,
        "spatial_invalid": spatial_invalid,
        "dataset_daily": aggregate_result,
        "lineage_summary": lineage_result,
        "load_control": load_result,
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
    parser.add_argument("--expected-replay-factor", type=int, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    with psycopg.connect(args.dsn, autocommit=True) as connection:
        result = verify(
            connection,
            args.expected_rows,
            expected_unique_detections=args.expected_unique_detections,
            expected_original=args.expected_original,
            expected_replay=args.expected_replay,
            expected_synthetic=args.expected_synthetic,
            expected_replay_factor=args.expected_replay_factor,
        )
    body = json.dumps(result, indent=2, sort_keys=True, default=str) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".part")
        temporary.write_text(body, encoding="utf-8")
        temporary.replace(args.output)
    print(body, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
