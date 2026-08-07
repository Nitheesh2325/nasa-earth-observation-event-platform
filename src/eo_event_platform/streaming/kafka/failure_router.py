"""Route bounded invalid and exhausted-processing events to governed Kafka topics."""

from __future__ import annotations

import argparse
import json
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition

from eo_event_platform.events.canonicalization import write_json_atomically

from .contracts import DLQ_TOPIC, REJECTED_TOPIC, diagnostic_consumer_config, producer_config
from .producer import partition_watermarks


ROUTING_ENVELOPE_VERSION = "1.0.0"
TEST_FAULT_MODE = "EXHAUST_RETRIES"
REQUIRED_TEXT_FIELDS = (
    "event_id",
    "lineage_root_id",
    "source_type",
    "source_dataset",
    "source_record_id",
    "ingestion_run_id",
    "event_timestamp",
    "ingestion_timestamp",
    "schema_version",
)


def validation_errors(event: dict[str, object], message_key: str | None) -> list[str]:
    """Return stable bounded routing errors without mutating the source event."""
    errors: list[str] = []
    for field in REQUIRED_TEXT_FIELDS:
        value = event.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"MISSING_{field.upper()}")
    if event.get("schema_version") != "1.0.0":
        errors.append("INVALID_SCHEMA_VERSION")
    if event.get("source_type") != "NASA_REPLAY" or event.get("is_synthetic") is not False:
        errors.append("INVALID_REPLAY_CLASSIFICATION")
    latitude = event.get("latitude")
    longitude = event.get("longitude")
    if not isinstance(latitude, (int, float)) or isinstance(latitude, bool) or not -90 <= latitude <= 90:
        errors.append("INVALID_LATITUDE")
    if not isinstance(longitude, (int, float)) or isinstance(longitude, bool) or not -180 <= longitude <= 180:
        errors.append("INVALID_LONGITUDE")
    if message_key != event.get("lineage_root_id"):
        errors.append("KAFKA_KEY_LINEAGE_MISMATCH")
    return sorted(set(errors))


def process_with_bounded_attempts(event: dict[str, object], max_attempts: int) -> tuple[bool, int, str | None]:
    """Exercise the processing retry boundary; fault injection is fixture-only."""
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")
    for attempt in range(1, max_attempts + 1):
        try:
            if event.get("_test_fault_mode") == TEST_FAULT_MODE:
                raise RuntimeError("controlled fixture processing failure")
            return True, attempt, None
        except RuntimeError as exc:
            if attempt == max_attempts:
                return False, attempt, type(exc).__name__
    raise AssertionError("bounded processing loop did not terminate")


def build_envelope(
    *,
    routing_type: str,
    routing_run_id: str,
    routed_at: str,
    source_message: object,
    event: dict[str, object] | None,
    reason_codes: list[str],
    processing_attempts: int,
    failure_category: str | None,
) -> dict[str, object]:
    return {
        "routing_envelope_version": ROUTING_ENVELOPE_VERSION,
        "routing_type": routing_type,
        "routing_run_id": routing_run_id,
        "routed_at": routed_at,
        "source_topic": source_message.topic(),  # type: ignore[attr-defined]
        "source_partition": source_message.partition(),  # type: ignore[attr-defined]
        "source_offset": source_message.offset(),  # type: ignore[attr-defined]
        "source_kafka_timestamp_ms": source_message.timestamp()[1],  # type: ignore[attr-defined]
        "message_key": source_message.key().decode("utf-8") if source_message.key() else None,  # type: ignore[attr-defined]
        "event_id": event.get("event_id") if event else None,
        "lineage_root_id": event.get("lineage_root_id") if event else None,
        "reason_codes": reason_codes,
        "processing_attempts": processing_attempts,
        "failure_category": failure_category,
        "original_event": event,
        "original_value_utf8": source_message.value().decode("utf-8", errors="replace"),  # type: ignore[attr-defined]
    }


def verify_routed_range(
    *,
    bootstrap_servers: str,
    topic: str,
    start_offsets: dict[int, int],
    end_offsets: dict[int, int],
    expected_count: int,
    expected_routing_type: str,
) -> dict[str, object]:
    consumer = Consumer(diagnostic_consumer_config(bootstrap_servers, f"eo-route-verify-{uuid.uuid4()}"))
    consumer.assign([TopicPartition(topic, partition, start_offsets[partition]) for partition in sorted(start_offsets)])
    consumed = 0
    invalid_envelopes = 0
    key_mismatches = 0
    source_coordinates: set[tuple[str, int, int]] = set()
    consumed_by_partition: Counter[int] = Counter()
    last_progress = time.monotonic()
    try:
        while consumed < expected_count:
            message = consumer.poll(1.0)
            if message is None:
                if time.monotonic() - last_progress > 15:
                    raise RuntimeError(f"timed out verifying {topic}")
                continue
            if message.error():
                raise RuntimeError(f"failed to verify {topic}: {message.error()}")
            partition = message.partition()
            if message.offset() < start_offsets[partition] or message.offset() >= end_offsets[partition]:
                continue
            last_progress = time.monotonic()
            consumed += 1
            consumed_by_partition[partition] += 1
            try:
                envelope = json.loads(message.value())
            except (json.JSONDecodeError, UnicodeDecodeError):
                invalid_envelopes += 1
                continue
            if envelope.get("routing_type") != expected_routing_type or envelope.get("routing_envelope_version") != ROUTING_ENVELOPE_VERSION:
                invalid_envelopes += 1
            key = message.key().decode("utf-8") if message.key() else None
            if key != envelope.get("lineage_root_id"):
                key_mismatches += 1
            source_coordinates.add((str(envelope.get("source_topic")), int(envelope.get("source_partition")), int(envelope.get("source_offset"))))
    finally:
        consumer.close()
    return {
        "consumed_count": consumed,
        "invalid_envelope_count": invalid_envelopes,
        "key_mismatch_count": key_mismatches,
        "unique_source_coordinate_count": len(source_coordinates),
        "consumed_by_partition": dict(sorted(consumed_by_partition.items())),
        "verified": consumed == expected_count and invalid_envelopes == 0 and key_mismatches == 0 and len(source_coordinates) == expected_count,
    }


def route_failures(args: argparse.Namespace) -> dict[str, object]:
    started_at = datetime.now(timezone.utc)
    started_clock = time.perf_counter()
    routing_run_id = str(uuid.uuid4())
    producer_manifest_path = Path(args.producer_manifest)
    source_manifest = json.loads(producer_manifest_path.read_text(encoding="utf-8"))
    if source_manifest.get("status") != "SUCCEEDED" or not source_manifest.get("reconciled"):
        raise RuntimeError("source producer manifest is not successful and reconciled")
    source_topic = str(source_manifest["topic"])
    starts = {int(k): int(v) for k, v in source_manifest["start_offsets"].items()}
    ends = {int(k): int(v) for k, v in source_manifest["end_offsets"].items()}
    expected_source_count = int(source_manifest["delivered_count"])

    output_topics = {"REJECTED": REJECTED_TOPIC, "DLQ": DLQ_TOPIC}
    output_starts = {
        routing_type: partition_watermarks(args.bootstrap_servers, topic, 3)
        for routing_type, topic in output_topics.items()
    }
    delivered: Counter[str] = Counter()
    delivered_by_partition: dict[str, Counter[int]] = {"REJECTED": Counter(), "DLQ": Counter()}
    delivery_failures: list[str] = []
    producer = Producer(producer_config(args.bootstrap_servers, f"eo-failure-router-{routing_run_id}"))

    def callback_for(routing_type: str):
        def on_delivery(error: object, message: object) -> None:
            if error is not None:
                delivery_failures.append(str(error))
                return
            delivered[routing_type] += 1
            delivered_by_partition[routing_type][message.partition()] += 1  # type: ignore[attr-defined]
        return on_delivery

    consumer = Consumer(diagnostic_consumer_config(args.bootstrap_servers, f"eo-failure-source-{routing_run_id}"))
    consumer.assign([TopicPartition(source_topic, partition, starts[partition]) for partition in sorted(starts)])
    source_consumed = 0
    passed_count = 0
    last_progress = time.monotonic()
    try:
        while source_consumed < expected_source_count:
            message = consumer.poll(1.0)
            if message is None:
                if time.monotonic() - last_progress > 15:
                    raise RuntimeError("timed out reading bounded failure fixture")
                continue
            if message.error():
                raise RuntimeError(f"source consume failed: {message.error()}")
            partition = message.partition()
            if message.offset() < starts[partition] or message.offset() >= ends[partition]:
                continue
            last_progress = time.monotonic()
            source_consumed += 1
            routed_at = datetime.now(timezone.utc).isoformat()
            key = message.key().decode("utf-8") if message.key() else None
            try:
                event = json.loads(message.value())
                if not isinstance(event, dict):
                    raise ValueError("event must be a JSON object")
                errors = validation_errors(event, key)
            except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
                event = None
                errors = ["CORRUPT_JSON"]

            if errors:
                routing_type = "REJECTED"
                envelope = build_envelope(
                    routing_type=routing_type,
                    routing_run_id=routing_run_id,
                    routed_at=routed_at,
                    source_message=message,
                    event=event,
                    reason_codes=errors,
                    processing_attempts=0,
                    failure_category=None,
                )
            else:
                processed, attempts, failure_category = process_with_bounded_attempts(event, args.max_processing_attempts)
                if processed:
                    passed_count += 1
                    continue
                routing_type = "DLQ"
                envelope = build_envelope(
                    routing_type=routing_type,
                    routing_run_id=routing_run_id,
                    routed_at=routed_at,
                    source_message=message,
                    event=event,
                    reason_codes=["PROCESSING_RETRIES_EXHAUSTED"],
                    processing_attempts=attempts,
                    failure_category=failure_category,
                )

            value = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
            while True:
                try:
                    producer.produce(
                        output_topics[routing_type],
                        key=str(envelope["lineage_root_id"]).encode("utf-8"),
                        value=value,
                        headers={"routing_type": routing_type, "routing_envelope_version": ROUTING_ENVELOPE_VERSION, "routing_run_id": routing_run_id},
                        on_delivery=callback_for(routing_type),
                    )
                    break
                except BufferError:
                    producer.poll(0.1)
            producer.poll(0)
    finally:
        consumer.close()

    undelivered = producer.flush(30)
    output_ends = {
        routing_type: partition_watermarks(args.bootstrap_servers, topic, 3)
        for routing_type, topic in output_topics.items()
    }
    offset_deltas = {
        routing_type: sum(output_ends[routing_type][p] - output_starts[routing_type][p] for p in output_starts[routing_type])
        for routing_type in output_topics
    }
    verifications = {
        routing_type: verify_routed_range(
            bootstrap_servers=args.bootstrap_servers,
            topic=topic,
            start_offsets=output_starts[routing_type],
            end_offsets=output_ends[routing_type],
            expected_count=delivered[routing_type],
            expected_routing_type=routing_type,
        )
        for routing_type, topic in output_topics.items()
    }
    reconciled = (
        source_consumed == passed_count + delivered["REJECTED"] + delivered["DLQ"]
        and not delivery_failures
        and undelivered == 0
        and all(offset_deltas[kind] == delivered[kind] for kind in output_topics)
        and all(result["verified"] for result in verifications.values())
    )
    manifest = {
        "routing_run_id": routing_run_id,
        "status": "SUCCEEDED" if reconciled else "FAILED",
        "pipeline_version": args.pipeline_version,
        "producer_manifest_path": producer_manifest_path.as_posix(),
        "source_producer_run_id": source_manifest["producer_run_id"],
        "source_topic": source_topic,
        "source_start_offsets": starts,
        "source_end_offsets": ends,
        "source_consumed_count": source_consumed,
        "passed_count": passed_count,
        "rejected_count": delivered["REJECTED"],
        "dlq_count": delivered["DLQ"],
        "max_processing_attempts": args.max_processing_attempts,
        "delivery_failure_count": len(delivery_failures),
        "undelivered_after_flush": undelivered,
        "output_start_offsets": output_starts,
        "output_end_offsets": output_ends,
        "output_offset_deltas": offset_deltas,
        "delivered_by_partition": {kind: dict(sorted(counts.items())) for kind, counts in delivered_by_partition.items()},
        "verifications": verifications,
        "reconciled": reconciled,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": time.perf_counter() - started_clock,
        "test_fault_injection_enabled": True,
    }
    manifest_path = Path(args.manifest_root) / f"run_date={started_at.date()}" / f"{routing_run_id}.json"
    write_json_atomically(manifest_path, manifest)
    if not reconciled:
        raise RuntimeError(f"failure routing did not reconcile: {manifest_path}")
    return {**manifest, "manifest_path": manifest_path.as_posix()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer-manifest", required=True)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--max-processing-attempts", type=int, default=3)
    parser.add_argument("--manifest-root", default="data/local/manifests/kafka_failure_router")
    parser.add_argument("--pipeline-version", required=True)
    parser.add_argument("--enable-test-fault-injection", action="store_true", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = route_failures(args)
    for key in ("routing_run_id", "status", "source_consumed_count", "passed_count", "rejected_count", "dlq_count", "reconciled", "duration_seconds", "manifest_path"):
        print(f"{key}={result[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
