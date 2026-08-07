"""Run a bounded Kafka-to-Bronze-and-Silver Structured Streaming query."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from eo_event_platform.events.canonicalization import write_json_atomically
from eo_event_platform.spark.batch.bronze_to_silver import validation_error_array
from eo_event_platform.spark.schemas import CANONICAL_EVENT_V1_SCHEMA


JOB_NAME = "kafka-to-silver-streaming-v1"
WATERMARK_DELAY = "10 minutes"


def starting_offsets_json(producer_manifest: dict[str, object]) -> str:
    """Return Spark's explicit Kafka starting-offset JSON from a producer manifest."""
    topic = str(producer_manifest["topic"])
    offsets = {
        str(partition): int(offset)
        for partition, offset in dict(producer_manifest["start_offsets"]).items()
    }
    return json.dumps({topic: offsets}, sort_keys=True, separators=(",", ":"))


def expected_end_offsets(producer_manifest: dict[str, object]) -> dict[int, int]:
    return {
        int(partition): int(offset)
        for partition, offset in dict(producer_manifest["end_offsets"]).items()
    }


def parsed_kafka_events(kafka_rows: DataFrame) -> DataFrame:
    """Parse canonical values and preserve broker metadata and key integrity."""
    parsed = kafka_rows.select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_value"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("broker_timestamp"),
        F.from_json(
            F.col("value").cast("string"),
            CANONICAL_EVENT_V1_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ).alias("event"),
    )
    events = parsed.select("message_key", "raw_value", "topic", "partition", "offset", "broker_timestamp", "event.*")
    return (
        events.withColumn("kafka_topic", F.col("topic"))
        .withColumn("kafka_partition", F.col("partition").cast("long"))
        .withColumn("kafka_offset", F.col("offset").cast("long"))
        .withColumn("kafka_timestamp", F.col("broker_timestamp"))
        .withColumn("key_lineage_mismatch", F.col("message_key") != F.col("lineage_root_id"))
        .withColumn("spark_validation_error_codes", validation_error_array())
        .withColumn(
            "spark_validation_error_codes",
            F.when(
                F.col("key_lineage_mismatch"),
                F.array_union(
                    F.col("spark_validation_error_codes"),
                    F.array(F.lit("KAFKA_KEY_LINEAGE_MISMATCH")),
                ),
            ).otherwise(F.col("spark_validation_error_codes")),
        )
        .withColumn(
            "spark_validation_status",
            F.when(F.size("spark_validation_error_codes") == 0, F.lit("ACCEPTED")).otherwise(F.lit("REJECTED")),
        )
    )


def source_stream(spark: SparkSession, args: argparse.Namespace, starting_offsets: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))
        .load()
    )


def run_available_now(stream: DataFrame, *, output_path: Path, checkpoint_path: Path) -> list[dict[str, object]]:
    query = (
        stream.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return [json.loads(item.json) for item in query.recentProgress]


def run_streaming(args: argparse.Namespace) -> dict[str, object]:
    started_at = datetime.now(timezone.utc)
    started_clock = time.perf_counter()
    producer_manifest_path = Path(args.producer_manifest)
    producer_manifest = json.loads(producer_manifest_path.read_text(encoding="utf-8"))
    if producer_manifest.get("status") != "SUCCEEDED" or not producer_manifest.get("reconciled"):
        raise RuntimeError("producer manifest is not successful and reconciled")
    if producer_manifest["topic"] != args.topic:
        raise RuntimeError("producer manifest topic does not match requested topic")
    expected_input = int(producer_manifest["delivered_count"])
    offsets_json = starting_offsets_json(producer_manifest)

    spark = (
        SparkSession.builder.appName(JOB_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    run_root = Path(args.output_root) / f"streaming_run_id={args.streaming_run_id}"
    landing_path = run_root / "bronze" / "kafka_events"
    accepted_path = run_root / "silver" / "events"
    rejected_path = run_root / "quarantine" / "rejected"
    checkpoint_root = Path(args.checkpoint_root) / args.streaming_run_id

    try:
        landing_progress = run_available_now(
            source_stream(spark, args, offsets_json).select(
                "topic", "partition", "offset", "timestamp", "timestampType", "key", "value", "headers"
            ),
            output_path=landing_path,
            checkpoint_path=checkpoint_root / "landing-v1",
        )

        valid = parsed_kafka_events(source_stream(spark, args, offsets_json)).filter(
            F.col("spark_validation_status") == "ACCEPTED"
        )
        accepted = (
            valid.withWatermark("scheduled_replay_timestamp", WATERMARK_DELAY)
            .dropDuplicatesWithinWatermark(["event_id"])
            .drop("raw_value", "message_key", "key_lineage_mismatch", "_corrupt_record")
            .withColumn("validation_status", F.lit("ACCEPTED"))
            .withColumn("validation_error_codes", F.array().cast("array<string>"))
            .withColumn("deduplication_status", F.lit("UNIQUE"))
        )
        accepted_progress = run_available_now(
            accepted,
            output_path=accepted_path,
            checkpoint_path=checkpoint_root / "accepted-v1",
        )

        rejected = parsed_kafka_events(source_stream(spark, args, offsets_json)).filter(
            F.col("spark_validation_status") == "REJECTED"
        )
        rejected_progress = run_available_now(
            rejected,
            output_path=rejected_path,
            checkpoint_path=checkpoint_root / "rejected-v1",
        )

        landing = spark.read.parquet(str(landing_path))
        accepted_readback = spark.read.parquet(str(accepted_path))
        rejected_readback = spark.read.parquet(str(rejected_path))
        landing_count = landing.count()
        accepted_count = accepted_readback.count()
        rejected_count = rejected_readback.count()
        duplicate_count = landing_count - accepted_count - rejected_count
        if min(landing_count, accepted_count, rejected_count, duplicate_count) < 0:
            raise RuntimeError("streaming outcome counts are invalid")
        reconciled = landing_count == expected_input and landing_count == accepted_count + rejected_count + duplicate_count
        if not reconciled:
            raise RuntimeError("streaming counts do not reconcile")

        observed_offsets = {
            int(row["partition"]): (int(row["minimum"]), int(row["maximum"]) + 1)
            for row in landing.groupBy("partition").agg(
                F.min("offset").alias("minimum"), F.max("offset").alias("maximum")
            ).collect()
        }
        expected_starts = {int(k): int(v) for k, v in dict(producer_manifest["start_offsets"]).items()}
        expected_ends = expected_end_offsets(producer_manifest)
        expected_nonempty = {
            partition: (expected_starts[partition], expected_ends[partition])
            for partition in expected_starts
            if expected_ends[partition] > expected_starts[partition]
        }
        offsets_reconciled = observed_offsets == expected_nonempty
        if not offsets_reconciled:
            raise RuntimeError("streaming Kafka offsets do not reconcile")

        manifest = {
            "streaming_run_id": args.streaming_run_id,
            "status": "SUCCEEDED",
            "job_name": JOB_NAME,
            "pipeline_version": args.pipeline_version,
            "spark_version": spark.version,
            "spark_master": spark.sparkContext.master,
            "connector_coordinate": "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2",
            "producer_manifest_path": producer_manifest_path.as_posix(),
            "producer_run_id": producer_manifest["producer_run_id"],
            "topic": args.topic,
            "starting_offsets": expected_starts,
            "ending_offsets": expected_ends,
            "observed_nonempty_offset_ranges": observed_offsets,
            "offsets_reconciled": offsets_reconciled,
            "input_count": landing_count,
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "duplicate_count": duplicate_count,
            "reconciled": reconciled,
            "watermark_column": "scheduled_replay_timestamp",
            "watermark_delay": WATERMARK_DELAY,
            "deduplication_key": "event_id",
            "landing_path": landing_path.as_posix(),
            "accepted_path": accepted_path.as_posix(),
            "rejected_path": rejected_path.as_posix(),
            "checkpoint_root": checkpoint_root.as_posix(),
            "landing_progress": landing_progress,
            "accepted_progress": accepted_progress,
            "rejected_progress": rejected_progress,
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": time.perf_counter() - started_clock,
            "shuffle_partitions": args.shuffle_partitions,
            "max_offsets_per_trigger": args.max_offsets_per_trigger,
        }
        manifest_path = Path(args.manifest_root) / f"run_date={started_at.date()}" / f"{args.streaming_run_id}.json"
        write_json_atomically(manifest_path, manifest)
        return {**manifest, "manifest_path": manifest_path.as_posix()}
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer-manifest", required=True)
    parser.add_argument("--streaming-run-id", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--checkpoint-root", required=True)
    parser.add_argument("--manifest-root", required=True)
    parser.add_argument("--pipeline-version", required=True)
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--topic", default="eo.events.replay.v1")
    parser.add_argument("--shuffle-partitions", type=int, default=4)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=100)
    parser.add_argument("--log-level", default="WARN")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.shuffle_partitions <= 0 or args.max_offsets_per_trigger <= 0:
        raise ValueError("streaming partition and offset limits must be positive")
    result = run_streaming(args)
    for key in ("streaming_run_id", "status", "input_count", "accepted_count", "rejected_count", "duplicate_count", "offsets_reconciled", "duration_seconds", "manifest_path"):
        print(f"{key}={result[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
