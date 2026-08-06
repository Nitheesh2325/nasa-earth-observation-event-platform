"""Validate, deduplicate, and write canonical Bronze events to Silver Parquet."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from eo_event_platform.common.metadata import detect_pipeline_version
from eo_event_platform.spark.schemas import CANONICAL_EVENT_V1_SCHEMA


JOB_NAME = "bronze-to-silver-batch-v1"
APPROVED_SOURCE_TYPES = ("NASA_ORIGINAL", "NASA_REPLAY", "SYNTHETIC_SCALE_TEST")


def _missing_text(column_name: str) -> Column:
    return F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")


def validation_error_array() -> Column:
    """Build stable validation reason codes entirely with Spark expressions."""
    checks: tuple[tuple[Column, str], ...] = (
        (F.col("_corrupt_record").isNotNull(), "CORRUPT_JSON"),
        (_missing_text("event_id"), "MISSING_EVENT_ID"),
        (_missing_text("detection_id"), "MISSING_DETECTION_ID"),
        (_missing_text("lineage_root_id"), "MISSING_LINEAGE_ROOT_ID"),
        (_missing_text("source_type"), "MISSING_SOURCE_TYPE"),
        (_missing_text("source_dataset"), "MISSING_SOURCE_DATASET"),
        (_missing_text("source_record_id"), "MISSING_SOURCE_RECORD_ID"),
        (_missing_text("ingestion_run_id"), "MISSING_INGESTION_RUN_ID"),
        (_missing_text("pipeline_version"), "MISSING_PIPELINE_VERSION"),
        (F.col("is_synthetic").isNull(), "MISSING_IS_SYNTHETIC"),
        (F.col("event_timestamp").isNull(), "MISSING_OR_INVALID_EVENT_TIMESTAMP"),
        (
            F.col("ingestion_timestamp").isNull(),
            "MISSING_OR_INVALID_INGESTION_TIMESTAMP",
        ),
        (
            F.col("latitude").isNull()
            | (F.col("latitude") < -90.0)
            | (F.col("latitude") > 90.0),
            "INVALID_LATITUDE",
        ),
        (
            F.col("longitude").isNull()
            | (F.col("longitude") < -180.0)
            | (F.col("longitude") > 180.0),
            "INVALID_LONGITUDE",
        ),
        (
            ~F.col("source_type").isin(*APPROVED_SOURCE_TYPES),
            "INVALID_SOURCE_TYPE",
        ),
        (F.col("schema_version") != "1.0.0", "INVALID_SCHEMA_VERSION"),
        (
            (F.col("source_type") == "NASA_ORIGINAL") & F.col("is_synthetic"),
            "ORIGINAL_MARKED_SYNTHETIC",
        ),
        (
            (F.col("source_type") == "NASA_ORIGINAL")
            & (
                (F.col("event_id") != F.col("detection_id"))
                | (F.col("event_id") != F.col("lineage_root_id"))
                | (F.col("event_id") != F.col("source_record_id"))
            ),
            "ORIGINAL_IDENTITY_MISMATCH",
        ),
        (
            (F.col("source_type") == "NASA_REPLAY")
            & (F.col("is_synthetic") | _missing_text("replay_run_id")),
            "INVALID_REPLAY_CLASSIFICATION",
        ),
        (
            (F.col("source_type") == "SYNTHETIC_SCALE_TEST")
            & ((~F.col("is_synthetic")) | _missing_text("synthetic_generation_id")),
            "INVALID_SYNTHETIC_CLASSIFICATION",
        ),
        (
            F.col("event_timestamp") > F.col("ingestion_timestamp"),
            "EVENT_AFTER_INGESTION",
        ),
        (~F.col("day_night").isin("D", "N"), "INVALID_DAY_NIGHT"),
        (
            F.col("bright_ti4_kelvin").isNotNull()
            & (F.col("bright_ti4_kelvin") <= 0.0),
            "NON_POSITIVE_BRIGHT_TI4",
        ),
        (
            F.col("bright_ti5_kelvin").isNotNull()
            & (F.col("bright_ti5_kelvin") <= 0.0),
            "NON_POSITIVE_BRIGHT_TI5",
        ),
        (
            F.col("fire_radiative_power_mw").isNotNull()
            & (F.col("fire_radiative_power_mw") < 0.0),
            "NEGATIVE_FIRE_RADIATIVE_POWER",
        ),
        (F.col("scan_km").isNotNull() & (F.col("scan_km") <= 0.0), "NON_POSITIVE_SCAN"),
        (
            F.col("track_km").isNotNull() & (F.col("track_km") <= 0.0),
            "NON_POSITIVE_TRACK",
        ),
    )
    return F.array_compact(F.array(*[F.when(condition, F.lit(code)) for condition, code in checks]))


def classify_events(events: DataFrame, *, run_id: str, processed_at: str) -> DataFrame:
    """Apply contract validation and stable event-id deduplication."""
    validated = events.withColumn("spark_validation_error_codes", validation_error_array())
    validated = validated.withColumn(
        "spark_validation_status",
        F.when(F.size("spark_validation_error_codes") == 0, F.lit("ACCEPTED")).otherwise(
            F.lit("REJECTED")
        ),
    )
    dedup_window = Window.partitionBy("event_id").orderBy(
        F.col("raw_row_number").asc_nulls_last(), F.col("raw_payload_hash").asc_nulls_last()
    )
    return (
        validated.withColumn(
            "event_id_occurrence",
            F.when(
                F.col("spark_validation_status") == "ACCEPTED",
                F.row_number().over(dedup_window),
            ),
        )
        .withColumn(
            "spark_outcome",
            F.when(F.col("spark_validation_status") == "REJECTED", F.lit("REJECTED"))
            .when(F.col("event_id_occurrence") > 1, F.lit("DUPLICATE"))
            .otherwise(F.lit("ACCEPTED")),
        )
        .withColumn("spark_processing_run_id", F.lit(run_id))
        .withColumn("processing_timestamp", F.lit(processed_at).cast("timestamp"))
    )


def silver_events(classified: DataFrame) -> DataFrame:
    """Project accepted unique events into the version 1 Silver contract."""
    return (
        classified.filter(F.col("spark_outcome") == "ACCEPTED")
        .drop("_corrupt_record", "event_id_occurrence", "spark_outcome")
        .withColumn("validation_status", F.lit("ACCEPTED"))
        .withColumn("validation_error_codes", F.array().cast("array<string>"))
        .withColumn("deduplication_status", F.lit("UNIQUE"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
        .withColumn("ingestion_date", F.to_date("ingestion_timestamp"))
        .withColumn(
            "geometry_wkt",
            F.concat(
                F.lit("POINT ("),
                F.col("longitude").cast("string"),
                F.lit(" "),
                F.col("latitude").cast("string"),
                F.lit(")"),
            ),
        )
    )


def quarantine_events(classified: DataFrame) -> DataFrame:
    """Preserve invalid candidates and Spark reason codes."""
    return (
        classified.filter(F.col("spark_outcome") == "REJECTED")
        .drop("event_id_occurrence", "spark_outcome")
        .withColumn("validation_status", F.lit("REJECTED"))
        .withColumn("validation_error_codes", F.col("spark_validation_error_codes"))
    )


def duplicate_events(classified: DataFrame) -> DataFrame:
    """Preserve valid repeated event messages outside trusted Silver."""
    return (
        classified.filter(F.col("spark_outcome") == "DUPLICATE")
        .drop("_corrupt_record", "spark_outcome")
        .withColumn("deduplication_status", F.lit("DUPLICATE"))
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parquet_metrics(path: Path) -> tuple[int, int]:
    files = list(path.rglob("*.parquet"))
    return len(files), sum(item.stat().st_size for item in files)


def write_manifest(path: Path, manifest: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".part")
    temporary_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary_path.replace(path)


def run_batch(args: argparse.Namespace) -> dict[str, object]:
    """Execute one measured, reconciled Bronze-to-Silver batch run."""
    started_clock = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    processed_at = started_at.isoformat()
    run_id = str(uuid.uuid4())
    pipeline_version = args.pipeline_version or detect_pipeline_version()

    spark = (
        SparkSession.builder.appName(JOB_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    input_path = Path(args.input_path)
    run_root = Path(args.output_root) / f"processing_run_id={run_id}"
    silver_path = run_root / "silver" / "events"
    rejected_path = run_root / "quarantine" / "rejected"
    duplicates_path = run_root / "quarantine" / "duplicates"
    manifest_path = Path(args.manifest_root) / f"run_date={started_at.date()}" / f"{run_id}.json"

    try:
        events = (
            spark.read.schema(CANONICAL_EVENT_V1_SCHEMA)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(str(input_path))
        )
        classified = classify_events(events, run_id=run_id, processed_at=processed_at).persist(
            StorageLevel.MEMORY_AND_DISK
        )

        input_count = classified.count()
        accepted = silver_events(classified).persist(StorageLevel.MEMORY_AND_DISK)
        rejected = quarantine_events(classified)
        duplicates = duplicate_events(classified)
        accepted_count = accepted.count()
        rejected_count = rejected.count()
        duplicate_count = duplicates.count()
        reconciled = input_count == accepted_count + rejected_count + duplicate_count
        if not reconciled:
            raise RuntimeError("Spark outcome counts do not reconcile")

        accepted.write.mode("errorifexists").partitionBy("event_date").parquet(
            str(silver_path)
        )
        rejected.write.mode("errorifexists").parquet(str(rejected_path))
        duplicates.write.mode("errorifexists").parquet(str(duplicates_path))

        silver_readback_count = spark.read.parquet(str(silver_path)).count()
        rejected_readback_count = spark.read.parquet(str(rejected_path)).count()
        duplicate_readback_count = spark.read.parquet(str(duplicates_path)).count()
        readback_reconciled = (
            silver_readback_count == accepted_count
            and rejected_readback_count == rejected_count
            and duplicate_readback_count == duplicate_count
        )
        if not readback_reconciled:
            raise RuntimeError("Parquet read-back counts do not reconcile")

        silver_file_count, silver_bytes = parquet_metrics(silver_path)
        duration_seconds = time.perf_counter() - started_clock
        manifest: dict[str, object] = {
            "processing_run_id": run_id,
            "status": "SUCCEEDED",
            "job_name": JOB_NAME,
            "pipeline_version": pipeline_version,
            "schema_version": "1.0.0",
            "spark_version": spark.version,
            "spark_master": spark.sparkContext.master,
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": duration_seconds,
            "throughput_records_per_second": input_count / duration_seconds,
            "input_path": input_path.as_posix(),
            "input_sha256": sha256_file(input_path),
            "input_count": input_count,
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "duplicate_count": duplicate_count,
            "reconciled": reconciled,
            "silver_path": silver_path.as_posix(),
            "rejected_path": rejected_path.as_posix(),
            "duplicates_path": duplicates_path.as_posix(),
            "silver_readback_count": silver_readback_count,
            "rejected_readback_count": rejected_readback_count,
            "duplicate_readback_count": duplicate_readback_count,
            "readback_reconciled": readback_reconciled,
            "silver_parquet_file_count": silver_file_count,
            "silver_parquet_bytes": silver_bytes,
            "shuffle_partitions": args.shuffle_partitions,
            "default_parallelism": spark.sparkContext.defaultParallelism,
        }
        write_manifest(manifest_path, manifest)
        return {**manifest, "manifest_path": manifest_path.as_posix()}
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--manifest-root", required=True)
    parser.add_argument("--pipeline-version")
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--log-level", default="WARN")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.shuffle_partitions <= 0:
        raise ValueError("shuffle-partitions must be positive")
    result = run_batch(args)
    for key in (
        "processing_run_id",
        "status",
        "input_count",
        "accepted_count",
        "rejected_count",
        "duplicate_count",
        "silver_readback_count",
        "duration_seconds",
        "throughput_records_per_second",
        "manifest_path",
    ):
        print(f"{key}={result[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

