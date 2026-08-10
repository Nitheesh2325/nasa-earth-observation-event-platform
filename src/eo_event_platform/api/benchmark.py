"""Run the fixed single-client Phase 8A API latency and plan workload."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import psycopg
from fastapi.testclient import TestClient

from eo_event_platform.api.app import create_app
from eo_event_platform.api.models import BoundingBoxQuery
from eo_event_platform.api.repository import ApiRepository


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round((len(ordered) - 1) * fraction))]


def timings(client: TestClient, path: str, params: dict[str, Any] | None, repeats: int) -> dict[str, float]:
    durations = []
    for _ in range(repeats):
        started = time.perf_counter()
        response = client.get(path, params=params)
        duration = (time.perf_counter() - started) * 1000
        if response.status_code != 200:
            raise RuntimeError(f"benchmark request failed: {path} {response.status_code}")
        durations.append(duration)
    return {
        "p50_ms": round(statistics.median(durations), 3),
        "p95_ms": round(percentile(durations, 0.95), 3),
        "p99_ms": round(percentile(durations, 0.99), 3),
    }


def plan_index_names(node: dict[str, Any]) -> list[str]:
    names = [node["Index Name"]] if node.get("Index Name") else []
    for child in node.get("Plans", []):
        names.extend(plan_index_names(child))
    return names


def run(dsn: str, repeats: int) -> dict[str, Any]:
    repository = ApiRepository(dsn)
    client = TestClient(create_app(repository))
    with psycopg.connect(dsn, autocommit=True) as connection:
        sample = connection.execute(
            """SELECT lineage_root_id, longitude, latitude,
              coalesce(scheduled_replay_timestamp,event_timestamp) AS activity_timestamp
              FROM serving.event_detail ORDER BY event_id LIMIT 1"""
        ).fetchone()
    activity = sample[3]
    bbox = BoundingBoxQuery(
        min_longitude=max(-180, sample[1] - 0.01),
        min_latitude=max(-90, sample[2] - 0.01),
        max_longitude=min(180, sample[1] + 0.01),
        max_latitude=min(90, sample[2] + 0.01),
        start_time=activity - timedelta(minutes=1),
        end_time=activity + timedelta(minutes=1),
        limit=100,
    )
    workloads = {
        "readiness": ("/health/ready", None),
        "platform_summary": ("/v1/summary", None),
        "daily_activity": (
            "/v1/daily",
            {"start_date": activity.date().isoformat(), "end_date": activity.date().isoformat()},
        ),
        "detection_lineage": (f"/v1/lineages/{sample[0]}", {"limit": 100}),
        "spatial_bbox": ("/v1/events/bbox", bbox.model_dump(exclude_none=True)),
    }
    # One warm-up request per endpoint precedes the measured samples.
    for path, params in workloads.values():
        response = client.get(path, params=params)
        if response.status_code != 200:
            raise RuntimeError(f"warm-up failed: {path} {response.status_code}")
    latency = {name: timings(client, path, params, repeats) for name, (path, params) in workloads.items()}

    statement, params = repository.bbox_sql_for_plan(bbox)
    with psycopg.connect(dsn, autocommit=True) as connection:
        plan = connection.execute(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement,
            params,
        ).fetchone()[0][0]
        role = connection.execute(
            """SELECT current_user, current_setting('transaction_read_only'),
              pg_has_role(current_user,'eo_api_readonly','member')"""
        ).fetchone()
    return {
        "profile": "local_in_process_single_client",
        "repeats": repeats,
        "latency": latency,
        "bbox_plan": {
            "planning_time_ms": plan["Planning Time"],
            "execution_time_ms": plan["Execution Time"],
            "index_names": plan_index_names(plan["Plan"]),
        },
        "role": {"current_user": role[0], "transaction_read_only": role[1], "readonly_member": role[2]},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--repeats", type=int, default=30, choices=range(1, 101))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args.dsn, args.repeats)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".part")
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(args.output)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
