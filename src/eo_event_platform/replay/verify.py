"""Independently verify a deterministic controlled-replay artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections import Counter
from dataclasses import fields
from datetime import timedelta
from pathlib import Path
from typing import Any

from .generator import format_utc_milliseconds, load_original_events, parse_utc_timestamp
from .identity import ReplayPlan, build_replay_event_id, build_replay_run_id


OVERWRITTEN_FIELDS = {
    "event_id", "parent_event_id", "source_type", "is_synthetic", "replay_run_id",
    "replay_iteration", "replay_sequence_number", "scheduled_replay_timestamp",
    "pipeline_version", "kafka_topic", "kafka_partition", "kafka_offset",
    "kafka_timestamp", "validation_status", "validation_error_codes",
    "deduplication_status", "enrichment_status",
}


def verify_manifest(manifest_path: Path) -> dict[str, Any]:
    started = time.perf_counter()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "SUCCEEDED":
        raise ValueError("replay manifest is not successful")

    plan_data = manifest["plan"]
    plan = ReplayPlan(**{field.name: plan_data[field.name] for field in fields(ReplayPlan)})
    replay_run_id = build_replay_run_id(plan)
    if manifest["replay_run_id"] != replay_run_id:
        raise ValueError("manifest replay run identity does not match its plan")

    source_path = Path(manifest["source_path"])
    events_path = Path(manifest["events_path"])
    originals = load_original_events(source_path, plan)
    expected_count = plan.source_record_count * plan.replay_factor
    schedule_start = parse_utc_timestamp(plan.scheduled_replay_start)
    digest = hashlib.sha256()
    detection_counts: Counter[str] = Counter()
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    count = 0

    with events_path.open("rb") as source:
        for count, raw_line in enumerate(source, start=1):
            digest.update(raw_line)
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError as error:
                raise ValueError(f"replay line {count} is not valid JSON") from error
            sequence = count - 1
            replay_iteration = sequence // plan.source_record_count + 1
            original = originals[sequence % plan.source_record_count]
            parent_event_id = str(original["event_id"])
            scheduled_timestamp = format_utc_milliseconds(
                schedule_start + timedelta(milliseconds=sequence * plan.scheduled_interval_milliseconds)
            )
            expected_event_id = build_replay_event_id(
                replay_run_id=replay_run_id,
                parent_event_id=parent_event_id,
                replay_iteration=replay_iteration,
            )
            expected_values = {
                "event_id": expected_event_id,
                "parent_event_id": parent_event_id,
                "source_type": "NASA_REPLAY",
                "is_synthetic": False,
                "replay_run_id": replay_run_id,
                "replay_iteration": replay_iteration,
                "replay_sequence_number": sequence,
                "scheduled_replay_timestamp": scheduled_timestamp,
                "pipeline_version": manifest["pipeline_version"],
                "kafka_topic": None,
                "kafka_partition": None,
                "kafka_offset": None,
                "kafka_timestamp": None,
                "validation_status": "PENDING",
                "validation_error_codes": [],
                "deduplication_status": "PENDING",
                "enrichment_status": "NOT_STARTED",
            }
            for name, expected in expected_values.items():
                if event.get(name) != expected:
                    raise ValueError(f"replay line {count} has invalid {name}")
            for name, expected in original.items():
                if name not in OVERWRITTEN_FIELDS and event.get(name) != expected:
                    raise ValueError(f"replay line {count} changed original field {name}")
            if set(event) != (set(original) | OVERWRITTEN_FIELDS):
                raise ValueError(f"replay line {count} has an unexpected field set")
            detection_counts[str(event["detection_id"])] += 1
            first_timestamp = first_timestamp or scheduled_timestamp
            last_timestamp = scheduled_timestamp

    if count != expected_count:
        raise ValueError(f"replay row count is {count}, expected {expected_count}")
    if set(detection_counts.values()) != {plan.replay_factor}:
        raise ValueError("detection replay frequencies do not match replay factor")
    output_sha256 = digest.hexdigest()
    if output_sha256 != manifest["output_sha256"]:
        raise ValueError("replay artifact checksum does not match manifest")
    if events_path.stat().st_size != manifest["output_bytes"]:
        raise ValueError("replay artifact size does not match manifest")
    if first_timestamp != manifest["first_scheduled_replay_timestamp"] or last_timestamp != manifest["last_scheduled_replay_timestamp"]:
        raise ValueError("replay schedule boundaries do not match manifest")

    duration = time.perf_counter() - started
    return {
        "status": "PASSED",
        "manifest_path": manifest_path.as_posix(),
        "events_path": events_path.as_posix(),
        "replay_run_id": replay_run_id,
        "output_count": count,
        "unique_event_id_count": count,
        "unique_detection_id_count": len(detection_counts),
        "events_per_detection": plan.replay_factor,
        "source_type_counts": {"NASA_REPLAY": count},
        "synthetic_true_count": 0,
        "first_replay_sequence_number": 0,
        "last_replay_sequence_number": count - 1,
        "first_scheduled_replay_timestamp": first_timestamp,
        "last_scheduled_replay_timestamp": last_timestamp,
        "output_bytes": events_path.stat().st_size,
        "output_sha256": output_sha256,
        "verification_duration_seconds": duration,
        "verification_throughput_records_per_second": count / duration,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    result = verify_manifest(parser.parse_args().manifest)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
