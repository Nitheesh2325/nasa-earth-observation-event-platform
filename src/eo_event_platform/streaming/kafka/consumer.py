"""Consume exactly the offset boundaries recorded by a producer manifest."""

from __future__ import annotations

import argparse
import json
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from eo_event_platform.events.canonicalization import write_json_atomically

from .contracts import diagnostic_consumer_config


def consume_producer_run(
    producer_manifest_path: Path,
    *,
    bootstrap_servers: str,
    manifest_root: Path,
    expected_source_type: str,
    expected_synthetic_true: int,
    expected_detection_count: int,
    expected_replay_factor: int,
    idle_timeout_seconds: int = 15,
) -> dict[str, object]:
    producer_manifest = json.loads(producer_manifest_path.read_text(encoding="utf-8"))
    if producer_manifest.get("status") != "SUCCEEDED" or not producer_manifest.get("reconciled"):
        raise RuntimeError("producer manifest is not successful and reconciled")
    topic = str(producer_manifest["topic"])
    starts = {int(k): int(v) for k, v in producer_manifest["start_offsets"].items()}
    ends = {int(k): int(v) for k, v in producer_manifest["end_offsets"].items()}
    expected_count = int(producer_manifest["delivered_count"])
    diagnostic_run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    consumer = Consumer(
        diagnostic_consumer_config(
            bootstrap_servers, f"eo-diagnostic-{diagnostic_run_id}"
        )
    )
    consumer.assign(
        [TopicPartition(topic, partition, starts[partition]) for partition in sorted(starts)]
    )
    consumed = 0
    invalid_json = 0
    key_lineage_mismatch = 0
    missing_event_id = 0
    event_ids: Counter[str] = Counter()
    detection_ids: Counter[str] = Counter()
    source_types: Counter[str] = Counter()
    replay_sequences: set[int] = set()
    replay_iterations: set[int] = set()
    synthetic_true = 0
    consumed_by_partition: Counter[int] = Counter()
    last_progress = time.monotonic()
    try:
        while consumed < expected_count:
            message = consumer.poll(1.0)
            if message is None:
                if time.monotonic() - last_progress > idle_timeout_seconds:
                    raise RuntimeError("diagnostic consumer reached bounded idle timeout")
                continue
            if message.error():
                raise RuntimeError(f"diagnostic consume failed: {message.error()}")
            partition = message.partition()
            if message.offset() < starts[partition] or message.offset() >= ends[partition]:
                continue
            last_progress = time.monotonic()
            consumed += 1
            consumed_by_partition[partition] += 1
            try:
                event = json.loads(message.value())
            except json.JSONDecodeError:
                invalid_json += 1
                continue
            event_id = event.get("event_id")
            if not isinstance(event_id, str) or not event_id:
                missing_event_id += 1
            else:
                event_ids[event_id] += 1
            detection_id = event.get("detection_id")
            if isinstance(detection_id, str) and detection_id:
                detection_ids[detection_id] += 1
            source_type = event.get("source_type")
            if isinstance(source_type, str):
                source_types[source_type] += 1
            synthetic_true += int(event.get("is_synthetic") is True)
            sequence = event.get("replay_sequence_number")
            if isinstance(sequence, int) and not isinstance(sequence, bool):
                replay_sequences.add(sequence)
            iteration = event.get("replay_iteration")
            if isinstance(iteration, int) and not isinstance(iteration, bool):
                replay_iterations.add(iteration)
            expected_key = event.get("lineage_root_id")
            actual_key = message.key().decode("utf-8") if message.key() else None
            if actual_key != expected_key:
                key_lineage_mismatch += 1
    finally:
        consumer.close()

    duplicate_message_count = sum(count - 1 for count in event_ids.values() if count > 1)
    quality_checks = {
        "consumed_matches_expected": consumed == expected_count,
        "partition_counts_reconcile": sum(consumed_by_partition.values()) == consumed,
        "invalid_json_zero": invalid_json == 0,
        "missing_event_id_zero": missing_event_id == 0,
        "key_lineage_mismatch_zero": key_lineage_mismatch == 0,
        "unique_event_ids_match": len(event_ids) == expected_count,
        "duplicate_messages_zero": duplicate_message_count == 0,
        "source_type_counts_match": dict(source_types) == {expected_source_type: expected_count},
        "synthetic_true_matches": synthetic_true == expected_synthetic_true,
        "unique_detections_match": len(detection_ids) == expected_detection_count,
        "detection_frequencies_match": set(detection_ids.values()) == {expected_replay_factor},
        "replay_sequences_complete": len(replay_sequences) == expected_count and min(replay_sequences, default=-1) == 0 and max(replay_sequences, default=-1) == expected_count - 1,
        "replay_iterations_complete": replay_iterations == set(range(1, expected_replay_factor + 1)),
    }
    reconciled = all(quality_checks.values())
    duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
    result = {
        "diagnostic_run_id": diagnostic_run_id,
        "status": "SUCCEEDED" if reconciled else "FAILED",
        "producer_run_id": producer_manifest["producer_run_id"],
        "producer_manifest_path": producer_manifest_path.as_posix(),
        "topic": topic,
        "start_offsets": starts,
        "end_offsets": ends,
        "expected_count": expected_count,
        "consumed_count": consumed,
        "consumed_by_partition": dict(sorted(consumed_by_partition.items())),
        "invalid_json_count": invalid_json,
        "missing_event_id_count": missing_event_id,
        "key_lineage_mismatch_count": key_lineage_mismatch,
        "unique_event_id_count": len(event_ids),
        "duplicate_message_count": duplicate_message_count,
        "unique_detection_id_count": len(detection_ids),
        "events_per_detection_values": sorted(set(detection_ids.values())),
        "source_type_counts": dict(sorted(source_types.items())),
        "synthetic_true_count": synthetic_true,
        "unique_replay_sequence_count": len(replay_sequences),
        "first_replay_sequence": min(replay_sequences, default=None),
        "last_replay_sequence": max(replay_sequences, default=None),
        "replay_iterations": sorted(replay_iterations),
        "quality_checks": quality_checks,
        "reconciled": reconciled,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": duration_seconds,
        "throughput_records_per_second": consumed / duration_seconds,
    }
    manifest_path = (
        manifest_root
        / f"run_date={started_at.date().isoformat()}"
        / f"{diagnostic_run_id}.json"
    )
    write_json_atomically(manifest_path, result)
    result["manifest_path"] = manifest_path.as_posix()
    if not reconciled:
        raise RuntimeError(f"diagnostic consumer did not reconcile: {manifest_path}")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer-manifest", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--expected-source-type", required=True)
    parser.add_argument("--expected-synthetic-true", type=int, required=True)
    parser.add_argument("--expected-detection-count", type=int, required=True)
    parser.add_argument("--expected-replay-factor", type=int, required=True)
    parser.add_argument(
        "--manifest-root",
        type=Path,
        default=Path("data/local/manifests/kafka_consumer"),
    )
    args = parser.parse_args()
    result = consume_producer_run(
        args.producer_manifest,
        bootstrap_servers=args.bootstrap_servers,
        manifest_root=args.manifest_root,
        expected_source_type=args.expected_source_type,
        expected_synthetic_true=args.expected_synthetic_true,
        expected_detection_count=args.expected_detection_count,
        expected_replay_factor=args.expected_replay_factor,
    )
    for key in (
        "diagnostic_run_id",
        "status",
        "expected_count",
        "consumed_count",
        "unique_event_id_count",
        "duplicate_message_count",
        "key_lineage_mismatch_count",
        "manifest_path",
    ):
        print(f"{key}={result[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
