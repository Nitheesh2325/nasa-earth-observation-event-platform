"""Independently verify a bounded Silver Parquet scale-gate output."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def utc_milliseconds(value: datetime) -> str:
    if value.tzinfo is None:
        return value.isoformat(timespec="milliseconds") + "Z"
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def verify(args: argparse.Namespace) -> dict[str, object]:
    started = time.perf_counter()
    spark = (
        SparkSession.builder.appName("silver-batch-gate-verifier-v1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    try:
        silver = spark.read.parquet(args.silver_path)
        rows = silver.count()
        unique_events = silver.select("event_id").distinct().count()
        unique_detections = silver.select("detection_id").distinct().count()
        truth = {
            row["source_type"]: row["count"]
            for row in silver.groupBy("source_type").count().collect()
        }
        ranges = silver.agg(
            F.min("replay_sequence_number").alias("min_sequence"),
            F.max("replay_sequence_number").alias("max_sequence"),
            F.min("replay_iteration").alias("min_iteration"),
            F.max("replay_iteration").alias("max_iteration"),
            F.min("scheduled_replay_timestamp").alias("first_scheduled"),
            F.max("scheduled_replay_timestamp").alias("last_scheduled"),
            F.sum(F.col("is_synthetic").cast("long")).alias("synthetic_true"),
            F.sum(F.col("parent_event_id").isNull().cast("long")).alias("null_parents"),
            F.sum(F.col("event_date").isNull().cast("long")).alias("null_event_dates"),
            F.sum(F.col("processing_timestamp").isNull().cast("long")).alias("null_processing_timestamps"),
            F.sum(F.col("spark_processing_run_id").isNull().cast("long")).alias("null_spark_run_ids"),
        ).first()
        actual = {
            "rows": rows,
            "unique_events": unique_events,
            "unique_detections": unique_detections,
            "source_type_counts": truth,
            "synthetic_true": ranges["synthetic_true"],
            "min_sequence": ranges["min_sequence"],
            "max_sequence": ranges["max_sequence"],
            "min_iteration": ranges["min_iteration"],
            "max_iteration": ranges["max_iteration"],
            "first_scheduled": utc_milliseconds(ranges["first_scheduled"]),
            "last_scheduled": utc_milliseconds(ranges["last_scheduled"]),
            "null_parents": ranges["null_parents"],
            "null_event_dates": ranges["null_event_dates"],
            "null_processing_timestamps": ranges["null_processing_timestamps"],
            "null_spark_run_ids": ranges["null_spark_run_ids"],
        }
        expected = {
            "rows": args.expected_rows,
            "unique_events": args.expected_rows,
            "unique_detections": args.expected_detections,
            "source_type_counts": {args.expected_source_type: args.expected_rows},
            "synthetic_true": args.expected_synthetic,
            "min_sequence": 0,
            "max_sequence": args.expected_rows - 1,
            "min_iteration": 1,
            "max_iteration": args.expected_replay_factor,
            "first_scheduled": args.expected_first_scheduled,
            "last_scheduled": args.expected_last_scheduled,
            "null_parents": 0,
            "null_event_dates": 0,
            "null_processing_timestamps": 0,
            "null_spark_run_ids": 0,
        }
        if actual != expected:
            raise RuntimeError(f"Silver truth verification failed: {actual}")
        duration = time.perf_counter() - started
        return {
            "status": "PASSED",
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "silver_path": args.silver_path,
            **actual,
            "duration_seconds": duration,
            "throughput_records_per_second": rows / duration,
            "spark_version": spark.version,
            "spark_master": spark.sparkContext.master,
            "shuffle_partitions": args.shuffle_partitions,
        }
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--expected-detections", type=int, required=True)
    parser.add_argument("--expected-replay-factor", type=int, required=True)
    parser.add_argument("--expected-source-type", required=True)
    parser.add_argument("--expected-synthetic", type=int, required=True)
    parser.add_argument("--expected-first-scheduled", required=True)
    parser.add_argument("--expected-last-scheduled", required=True)
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    parser.add_argument("--log-level", default="WARN")
    parser.add_argument("--evidence-path", type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if min(args.expected_rows, args.expected_detections, args.expected_replay_factor) <= 0:
        raise ValueError("expected counts and replay factor must be positive")
    result = verify(args)
    body = json.dumps(result, indent=2, sort_keys=True, default=str) + "\n"
    if args.evidence_path:
        args.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.evidence_path.with_suffix(args.evidence_path.suffix + ".part")
        temporary.write_text(body, encoding="utf-8")
        temporary.replace(args.evidence_path)
    print(body, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
