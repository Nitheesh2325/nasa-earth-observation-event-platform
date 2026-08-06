"""Bounded delivery-accounted producer for governed JSONL event artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import confluent_kafka
from confluent_kafka import Consumer, Producer, TopicPartition

from eo_event_platform.events.canonicalization import write_json_atomically

from .contracts import REPLAY_TOPIC, diagnostic_consumer_config, producer_config


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def partition_watermarks(
    bootstrap_servers: str, topic: str, partition_count: int
) -> dict[int, int]:
    consumer = Consumer(
        diagnostic_consumer_config(
            bootstrap_servers, f"eo-watermark-{uuid.uuid4()}"
        )
    )
    try:
        return {
            partition: consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10, cached=False
            )[1]
            for partition in range(partition_count)
        }
    finally:
        consumer.close()


def publish_jsonl(
    source_path: Path,
    *,
    expected_source_sha256: str,
    topic: str,
    partition_count: int,
    message_limit: int,
    bootstrap_servers: str,
    manifest_root: Path,
    pipeline_version: str,
) -> dict[str, object]:
    """Publish at most ``message_limit`` records and reconcile delivery offsets."""
    if message_limit <= 0:
        raise ValueError("message_limit must be positive")
    actual_sha256 = sha256_file(source_path)
    if actual_sha256 != expected_source_sha256:
        raise RuntimeError("producer input checksum does not match the approved artifact")

    started_at = datetime.now(timezone.utc)
    started_clock = time.perf_counter()
    producer_run_id = str(uuid.uuid4())
    start_offsets = partition_watermarks(bootstrap_servers, topic, partition_count)
    delivered_by_partition: Counter[int] = Counter()
    delivery_failures: list[str] = []

    producer = Producer(
        producer_config(bootstrap_servers, f"eo-replay-producer-{producer_run_id}")
    )

    def on_delivery(error: object, message: object) -> None:
        if error is not None:
            delivery_failures.append(str(error))
            return
        delivered_by_partition[message.partition()] += 1  # type: ignore[attr-defined]

    attempted = 0
    value_bytes = 0
    with source_path.open("rb") as source:
        for raw_line in source:
            if attempted >= message_limit:
                break
            value = raw_line.rstrip(b"\r\n")
            event = json.loads(value)
            lineage_root_id = event.get("lineage_root_id")
            if not isinstance(lineage_root_id, str) or not lineage_root_id:
                raise RuntimeError(f"message {attempted} has no lineage_root_id")
            while True:
                try:
                    producer.produce(
                        topic,
                        key=lineage_root_id.encode("utf-8"),
                        value=value,
                        headers={
                            "schema_version": str(event.get("schema_version", "1.0.0")),
                            "source_type": str(event.get("source_type", "UNKNOWN")),
                            "replay_contract_version": "1",
                            "producer_run_id": producer_run_id,
                        },
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)
            producer.poll(0)
            attempted += 1
            value_bytes += len(value)

    undelivered_after_flush = producer.flush(30)
    delivered = sum(delivered_by_partition.values())
    end_offsets = partition_watermarks(bootstrap_servers, topic, partition_count)
    offset_delta_by_partition = {
        partition: end_offsets[partition] - start_offsets[partition]
        for partition in range(partition_count)
    }
    offset_delta = sum(offset_delta_by_partition.values())
    duration_seconds = time.perf_counter() - started_clock
    reconciled = (
        attempted == delivered
        and not delivery_failures
        and undelivered_after_flush == 0
        and offset_delta == delivered
    )
    manifest = {
        "producer_run_id": producer_run_id,
        "status": "SUCCEEDED" if reconciled else "FAILED",
        "pipeline_version": pipeline_version,
        "bootstrap_servers": bootstrap_servers,
        "topic": topic,
        "partition_count": partition_count,
        "source_path": source_path.as_posix(),
        "source_sha256": actual_sha256,
        "message_limit": message_limit,
        "attempted_count": attempted,
        "delivered_count": delivered,
        "delivery_failure_count": len(delivery_failures),
        "delivery_failures": delivery_failures[:10],
        "undelivered_after_flush": undelivered_after_flush,
        "value_bytes": value_bytes,
        "delivered_by_partition": dict(sorted(delivered_by_partition.items())),
        "start_offsets": start_offsets,
        "end_offsets": end_offsets,
        "offset_delta_by_partition": offset_delta_by_partition,
        "offset_delta": offset_delta,
        "reconciled": reconciled,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": duration_seconds,
        "throughput_records_per_second": delivered / duration_seconds,
        "confluent_kafka_version": confluent_kafka.version(),
        "librdkafka_version": confluent_kafka.libversion(),
    }
    manifest_path = (
        manifest_root
        / f"run_date={started_at.date().isoformat()}"
        / f"{producer_run_id}.json"
    )
    write_json_atomically(manifest_path, manifest)
    manifest["manifest_path"] = manifest_path.as_posix()
    if not reconciled:
        raise RuntimeError(f"Kafka producer run did not reconcile: {manifest_path}")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-path", type=Path, required=True)
    parser.add_argument("--expected-source-sha256", required=True)
    parser.add_argument("--topic", default=REPLAY_TOPIC)
    parser.add_argument("--partition-count", type=int, default=6)
    parser.add_argument("--message-limit", type=int, required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument(
        "--manifest-root",
        type=Path,
        default=Path("data/local/manifests/kafka_producer"),
    )
    parser.add_argument("--pipeline-version", required=True)
    args = parser.parse_args()
    result = publish_jsonl(
        args.source_path,
        expected_source_sha256=args.expected_source_sha256,
        topic=args.topic,
        partition_count=args.partition_count,
        message_limit=args.message_limit,
        bootstrap_servers=args.bootstrap_servers,
        manifest_root=args.manifest_root,
        pipeline_version=args.pipeline_version,
    )
    for key in (
        "producer_run_id",
        "status",
        "attempted_count",
        "delivered_count",
        "offset_delta",
        "duration_seconds",
        "throughput_records_per_second",
        "manifest_path",
    ):
        print(f"{key}={result[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

