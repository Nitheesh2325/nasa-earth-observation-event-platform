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
            expected_key = event.get("lineage_root_id")
            actual_key = message.key().decode("utf-8") if message.key() else None
            if actual_key != expected_key:
                key_lineage_mismatch += 1
    finally:
        consumer.close()

    duplicate_message_count = sum(count - 1 for count in event_ids.values() if count > 1)
    reconciled = consumed == expected_count and sum(consumed_by_partition.values()) == consumed
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
        "reconciled": reconciled,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
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

