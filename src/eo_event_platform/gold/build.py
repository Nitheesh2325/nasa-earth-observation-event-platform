"""Build compact, manifest-governed Gold event detail from accepted Silver Parquet."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from eo_event_platform.common.metadata import detect_pipeline_version


JOB_NAME = "silver-to-gold-serving-v1"
GOLD_CONTRACT_VERSION = "1.1.0"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_metrics(path: Path, *, count_rows: bool) -> tuple[int, str, int | None]:
    digest = hashlib.sha256()
    rows = 0
    size = 0
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
            if count_rows:
                rows += chunk.count(b"\n")
    return size, digest.hexdigest(), rows if count_rows else None


def artifact_entries(root: Path) -> list[dict[str, object]]:
    entries = []
    for path in sorted(item for item in root.rglob("*") if item.is_file() and not item.name.startswith(".")):
        relative = path.relative_to(root).as_posix()
        size, digest, rows = file_metrics(
            path,
            count_rows=relative.startswith("load_artifact/part-") and relative.endswith(".json"),
        )
        entry: dict[str, object] = {"path": relative, "bytes": size, "sha256": digest}
        if rows is not None:
            entry["rows"] = rows
        entries.append(entry)
    return entries


def artifact_part_count(artifacts: list[dict[str, object]], prefix: str, suffix: str) -> int:
    return sum(
        str(artifact["path"]).startswith(prefix)
        and str(artifact["path"]).endswith(suffix)
        for artifact in artifacts
    )


def governed_content_hash(frame_columns: list[str]):
    excluded = {"gold_run_id", "gold_contract_version", "governed_content_hash"}
    ordered = [F.col(name) for name in sorted(frame_columns) if name not in excluded]
    return F.sha2(F.to_json(F.struct(*ordered), options={"ignoreNullFields": "false"}), 256)


def run(args: argparse.Namespace) -> dict[str, object]:
    started = datetime.now(timezone.utc)
    clock = time.perf_counter()
    gold_run_id = str(uuid.uuid4())
    pipeline_version = args.pipeline_version or detect_pipeline_version()
    output_root = Path(args.output_root) / f"gold_run_id={gold_run_id}"
    parquet_path = output_root / "event_detail"
    load_path = output_root / "load_artifact"
    manifest_path = output_root / "manifest.json"
    if output_root.exists():
        raise FileExistsError(output_root)

    spark = (
        SparkSession.builder.appName(JOB_NAME)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.output_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(args.log_level)
    try:
        silver = spark.read.parquet(args.silver_path)
        input_count = silver.count()
        if input_count != args.expected_rows:
            raise RuntimeError(f"expected {args.expected_rows} Silver rows, found {input_count}")
        if silver.select("event_id").distinct().count() != input_count:
            raise RuntimeError("Silver event_id values are not unique")
        classified = {row["source_type"]: row["count"] for row in silver.groupBy("source_type").count().collect()}
        synthetic_count = silver.filter(F.col("is_synthetic")).count()

        gold = (
            silver.withColumn("gold_run_id", F.lit(gold_run_id))
            .withColumn("gold_contract_version", F.lit(GOLD_CONTRACT_VERSION))
            .withColumn("gold_pipeline_version", F.lit(pipeline_version))
        )
        gold = gold.withColumn("governed_content_hash", governed_content_hash(gold.columns))
        gold.coalesce(args.output_partitions).write.mode("errorifexists").parquet(str(parquet_path))
        gold.coalesce(args.load_partitions).write.mode("errorifexists").json(str(load_path))
        parquet_readback = spark.read.parquet(str(parquet_path)).count()
        load_readback = spark.read.json(str(load_path)).count()
        if parquet_readback != input_count or load_readback != input_count:
            raise RuntimeError("Gold read-back counts do not reconcile")

        artifacts = artifact_entries(output_root)
        gold_parquet_files = artifact_part_count(artifacts, "event_detail/part-", ".parquet")
        load_artifact_files = artifact_part_count(artifacts, "load_artifact/part-", ".json")
        if gold_parquet_files <= 0 or load_artifact_files <= 0:
            raise RuntimeError("Gold data artifacts are missing")
        completed = datetime.now(timezone.utc)
        manifest = {
            "gold_run_id": gold_run_id,
            "gold_contract_version": GOLD_CONTRACT_VERSION,
            "pipeline_version": pipeline_version,
            "job_name": JOB_NAME,
            "status": "SUCCEEDED",
            "source_silver_path": Path(args.silver_path).as_posix(),
            "expected_rows": args.expected_rows,
            "input_rows": input_count,
            "gold_parquet_rows": parquet_readback,
            "load_artifact_rows": load_readback,
            "requested_gold_parquet_partitions": args.output_partitions,
            "requested_load_artifact_partitions": args.load_partitions,
            "gold_parquet_files": gold_parquet_files,
            "load_artifact_files": load_artifact_files,
            "source_type_counts": classified,
            "synthetic_rows": synthetic_count,
            "started_at": started.isoformat(),
            "completed_at": completed.isoformat(),
            "duration_seconds": time.perf_counter() - clock,
            "artifacts": artifacts,
        }
        temporary = manifest_path.with_suffix(".json.part")
        temporary.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(manifest_path)
        return {**manifest, "manifest_path": manifest_path.as_posix()}
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--output-partitions", type=int, default=2)
    parser.add_argument("--load-partitions", type=int, default=1)
    parser.add_argument("--pipeline-version")
    parser.add_argument("--log-level", default="WARN")
    args = parser.parse_args()
    if args.expected_rows <= 0 or args.output_partitions <= 0 or args.load_partitions <= 0:
        raise ValueError("row and partition counts must be positive")
    result = run(args)
    for key in ("gold_run_id", "input_rows", "gold_parquet_rows", "load_artifact_rows", "duration_seconds", "manifest_path"):
        print(f"{key}={result[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
