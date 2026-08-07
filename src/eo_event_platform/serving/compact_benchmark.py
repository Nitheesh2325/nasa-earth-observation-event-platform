"""Run the bounded full-payload versus compact PostgreSQL serving A/B gate."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from eo_event_platform.serving.postgres_loader import apply_migration
from eo_event_platform.serving.verify import explain, timed_query


FULL_TABLE = "event_detail"
COMPACT_TABLE = "event_detail_compact"


def common_columns(connection: psycopg.Connection[Any]) -> list[str]:
    rows = connection.execute(
        """SELECT a.attname
           FROM pg_attribute a
           WHERE a.attrelid = 'serving.event_detail_compact'::regclass
             AND a.attnum > 0 AND NOT a.attisdropped
           ORDER BY a.attnum"""
    ).fetchall()
    columns = [row[0] for row in rows]
    if "event_payload" in columns or not {"event_id", "governed_content_hash", "gold_run_id"}.issubset(columns):
        raise RuntimeError("compact column contract is invalid")
    return columns


def materialize_compact(connection: psycopg.Connection[Any], columns: list[str]) -> dict[str, float | int]:
    identifiers = sql.SQL(", ").join(sql.Identifier(column) for column in columns)
    statement = sql.SQL(
        "INSERT INTO serving.event_detail_compact ({columns}) "
        "SELECT {columns} FROM serving.event_detail "
        "ON CONFLICT (event_id) DO NOTHING"
    ).format(columns=identifiers)
    started = time.perf_counter()
    inserted = connection.execute(statement).rowcount
    connection.execute("ANALYZE serving.event_detail_compact")
    duration = time.perf_counter() - started
    return {"inserted_rows": inserted, "duration_seconds": duration}


def relation_sizes(connection: psycopg.Connection[Any], table: str) -> dict[str, int]:
    qualified = f"serving.{table}"
    row = connection.execute(
        """SELECT pg_total_relation_size(%s::regclass), pg_relation_size(%s::regclass),
          pg_indexes_size(%s::regclass),
          CASE WHEN c.reltoastrelid = 0 THEN 0 ELSE pg_total_relation_size(c.reltoastrelid) END
          FROM pg_class c WHERE c.oid = %s::regclass""",
        (qualified, qualified, qualified, qualified),
    ).fetchone()
    return {"total": row[0], "heap": row[1], "indexes": row[2], "toast": row[3]}


def plan_summary(plan: dict[str, Any]) -> dict[str, Any]:
    nodes: list[str] = []

    def visit(node: dict[str, Any]) -> None:
        nodes.append(node["Node Type"])
        for child in node.get("Plans", []):
            visit(child)

    visit(plan["Plan"])
    return {
        "execution_time_ms": plan["Execution Time"],
        "node_types": nodes,
        "shared_hit_blocks": plan["Plan"].get("Shared Hit Blocks", 0),
        "shared_read_blocks": plan["Plan"].get("Shared Read Blocks", 0),
    }


def benchmark_queries(connection: psycopg.Connection[Any], table: str) -> dict[str, Any]:
    identifier = sql.Identifier("serving", table)
    sample_lineage = connection.execute(
        sql.SQL("SELECT lineage_root_id FROM {} ORDER BY event_id LIMIT 1").format(identifier)
    ).fetchone()[0]
    queries = {
        "summary": (sql.SQL("SELECT source_type,count(*) FROM {} GROUP BY source_type").format(identifier), ()),
        "spatial_bbox": (sql.SQL("SELECT count(*) FROM {} WHERE geometry && ST_MakeEnvelope(-20,-20,20,20,4326)").format(identifier), ()),
        "lineage": (sql.SQL("SELECT event_id,event_timestamp FROM {} WHERE lineage_root_id=%s ORDER BY scheduled_replay_timestamp NULLS LAST").format(identifier), (sample_lineage,)),
    }
    result: dict[str, Any] = {}
    for name, (query, params) in queries.items():
        query_text = query.as_string(connection)
        result[name] = {
            "latency": timed_query(connection, query_text, params),
            "plan": plan_summary(explain(connection, query_text, params)),
        }
    return result


def run(connection: psycopg.Connection[Any], expected_rows: int) -> dict[str, Any]:
    full_rows = connection.execute("SELECT count(*) FROM serving.event_detail").fetchone()[0]
    if full_rows != expected_rows:
        raise RuntimeError(f"control table has {full_rows} rows, expected {expected_rows}")
    columns = common_columns(connection)
    connection.execute("TRUNCATE serving.event_detail_compact")
    first = materialize_compact(connection, columns)
    second = materialize_compact(connection, columns)
    compact_rows = connection.execute("SELECT count(*) FROM serving.event_detail_compact").fetchone()[0]
    if first["inserted_rows"] != expected_rows or second["inserted_rows"] != 0 or compact_rows != expected_rows:
        raise RuntimeError("compact materialization does not reconcile")

    unequal_rows = connection.execute(
        """SELECT count(*) FROM serving.event_detail a
           JOIN serving.event_detail_compact b USING (event_id)
           WHERE (to_jsonb(a) - 'event_payload') IS DISTINCT FROM to_jsonb(b)"""
    ).fetchone()[0]
    missing_rows = connection.execute(
        """SELECT count(*) FROM serving.event_detail a
           FULL JOIN serving.event_detail_compact b USING (event_id)
           WHERE a.event_id IS NULL OR b.event_id IS NULL"""
    ).fetchone()[0]
    if unequal_rows or missing_rows:
        raise RuntimeError("compact rows differ beyond event_payload removal")

    truth = connection.execute(
        """SELECT count(*),count(DISTINCT event_id),count(DISTINCT detection_id),
          count(*) FILTER (WHERE source_type='NASA_REPLAY'),
          count(*) FILTER (WHERE is_synthetic),
          count(*) FILTER (WHERE geometry IS NULL OR ST_SRID(geometry)<>4326
            OR ST_X(geometry)<>longitude OR ST_Y(geometry)<>latitude)
          FROM serving.event_detail_compact"""
    ).fetchone()
    if truth != (expected_rows, expected_rows, 10_000, expected_rows, 0, 0):
        raise RuntimeError(f"compact truth or geometry failed: {truth}")

    with connection.transaction(force_rollback=True):
        conflict_count = connection.execute(
            """SELECT count(*) FROM serving.event_detail_compact
               WHERE event_id=(SELECT min(event_id) FROM serving.event_detail_compact)
                 AND governed_content_hash <> 'conflict'"""
        ).fetchone()[0]
        if conflict_count != 1:
            raise RuntimeError("compact hash conflict probe failed")

    return {
        "control_rows": full_rows,
        "compact_rows": compact_rows,
        "columns_retained": len(columns),
        "columns_removed": ["event_payload"],
        "first_materialization": first,
        "second_materialization": second,
        "unequal_rows": unequal_rows,
        "missing_rows": missing_rows,
        "truth": {
            "rows": truth[0], "unique_events": truth[1], "unique_detections": truth[2],
            "replay": truth[3], "synthetic": truth[4], "invalid_geometry": truth[5],
        },
        "content_conflicts_detected": conflict_count,
        "sizes": {
            "full": relation_sizes(connection, FULL_TABLE),
            "compact": relation_sizes(connection, COMPACT_TABLE),
        },
        "queries": {
            "full": benchmark_queries(connection, FULL_TABLE),
            "compact": benchmark_queries(connection, COMPACT_TABLE),
        },
        "api_can_select_compact": connection.execute(
            "SELECT has_table_privilege('eo_api_readonly','serving.event_detail_compact','SELECT')"
        ).fetchone()[0],
        "api_can_insert_compact": connection.execute(
            "SELECT has_table_privilege('eo_api_readonly','serving.event_detail_compact','INSERT')"
        ).fetchone()[0],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--migration", type=Path, required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    args = parser.parse_args()
    with psycopg.connect(args.dsn, autocommit=False) as connection:
        apply_migration(connection, args.migration)
        connection.commit()
        result = run(connection, args.expected_rows)
        connection.commit()
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
