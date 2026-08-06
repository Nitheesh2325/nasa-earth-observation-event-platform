"""Streaming deterministic JSONL generation for controlled NASA replay."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from eo_event_platform.events.canonicalization import write_json_atomically

from .identity import ReplayPlan, build_replay_event_id, build_replay_run_id


NASA_ORIGINAL = "NASA_ORIGINAL"
NASA_REPLAY = "NASA_REPLAY"


class ReplayGenerationError(RuntimeError):
    """Raised when a replay artifact cannot satisfy its governed contract."""


@dataclass(frozen=True)
class ReplayGenerationResult:
    """Safe output metadata for one physical generation execution."""

    execution_run_id: str
    replay_run_id: str
    output_count: int
    output_sha256: str
    output_bytes: int
    events_path: str
    manifest_path: str


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_utc_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def format_utc_milliseconds(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def load_original_events(source_path: Path, plan: ReplayPlan) -> list[dict[str, object]]:
    """Verify the admitted original input before any replay output is created."""
    actual_sha256 = sha256_file(source_path)
    if actual_sha256 != plan.source_input_sha256:
        raise ReplayGenerationError("source checksum does not match replay plan")

    originals: list[dict[str, object]] = []
    seen_event_ids: set[str] = set()
    with source_path.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ReplayGenerationError(
                    f"source line {line_number} is not valid JSON"
                ) from exc
            event_id = event.get("event_id")
            if not isinstance(event_id, str) or not event_id:
                raise ReplayGenerationError(f"source line {line_number} has no event_id")
            if event_id in seen_event_ids:
                raise ReplayGenerationError(f"duplicate source event_id: {event_id}")
            if event.get("source_type") != NASA_ORIGINAL:
                raise ReplayGenerationError("replay source contains a non-original event")
            if event.get("is_synthetic") is not False:
                raise ReplayGenerationError("replay source contains a synthetic event")
            if not all(
                event.get(field_name) == event_id
                for field_name in ("detection_id", "lineage_root_id", "source_record_id")
            ):
                raise ReplayGenerationError("original source identity invariant failed")
            seen_event_ids.add(event_id)
            originals.append(event)

    if len(originals) != plan.source_record_count:
        raise ReplayGenerationError(
            f"source count {len(originals)} does not match plan {plan.source_record_count}"
        )
    originals.sort(key=lambda event: str(event["event_id"]))
    return originals


def build_replay_event(
    original: dict[str, object],
    *,
    replay_run_id: str,
    replay_iteration: int,
    replay_sequence_number: int,
    scheduled_replay_timestamp: str,
    pipeline_version: str,
) -> dict[str, object]:
    """Create one replay message without mutating original lineage or measurements."""
    parent_event_id = str(original["event_id"])
    replay = dict(original)
    replay.update(
        {
            "event_id": build_replay_event_id(
                replay_run_id=replay_run_id,
                parent_event_id=parent_event_id,
                replay_iteration=replay_iteration,
            ),
            "parent_event_id": parent_event_id,
            "source_type": NASA_REPLAY,
            "is_synthetic": False,
            "replay_run_id": replay_run_id,
            "replay_iteration": replay_iteration,
            "replay_sequence_number": replay_sequence_number,
            "scheduled_replay_timestamp": scheduled_replay_timestamp,
            "pipeline_version": pipeline_version,
            "kafka_topic": None,
            "kafka_partition": None,
            "kafka_offset": None,
            "kafka_timestamp": None,
            "validation_status": "PENDING",
            "validation_error_codes": [],
            "deduplication_status": "PENDING",
            "enrichment_status": "NOT_STARTED",
        }
    )
    return replay


def generate_replay(
    source_path: Path,
    *,
    plan: ReplayPlan,
    output_root: Path,
    manifest_root: Path,
    pipeline_version: str,
) -> ReplayGenerationResult:
    """Generate one immutable physical execution of a deterministic replay plan."""
    started_clock = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    execution_run_id = str(uuid.uuid4())
    replay_run_id = build_replay_run_id(plan)
    replay_run_id_sha256 = replay_run_id.rsplit(":", 1)[-1]
    run_date = started_at.date().isoformat()
    run_directory = (
        output_root
        / "source_type=NASA_REPLAY"
        / "replay_plan_version=1"
        / f"replay_run_id_sha256={replay_run_id_sha256}"
        / f"execution_run_id={execution_run_id}"
    )
    events_path = run_directory / "events.jsonl"
    temporary_path = events_path.with_suffix(".jsonl.part")
    manifest_path = (
        manifest_root / "replay" / f"run_date={run_date}" / f"{execution_run_id}.json"
    )

    failure_manifest: dict[str, object] = {
        "execution_run_id": execution_run_id,
        "replay_run_id": replay_run_id,
        "status": "FAILED",
        "pipeline_version": pipeline_version,
        "source_path": source_path.as_posix(),
        "plan": asdict(plan),
        "started_at": started_at.isoformat(),
    }

    try:
        originals = load_original_events(source_path, plan)
        schedule_start = parse_utc_timestamp(plan.scheduled_replay_start)
        expected_output_count = plan.source_record_count * plan.replay_factor
        output_digest = hashlib.sha256()
        output_bytes = 0
        output_count = 0
        unique_event_ids: set[str] = set()
        unique_detection_ids = {str(event["detection_id"]) for event in originals}
        first_scheduled_timestamp: str | None = None
        last_scheduled_timestamp: str | None = None

        events_path.parent.mkdir(parents=True, exist_ok=False)
        with temporary_path.open("wb") as output:
            for replay_iteration in range(1, plan.replay_factor + 1):
                for original_index, original in enumerate(originals):
                    sequence = (
                        (replay_iteration - 1) * plan.source_record_count
                    ) + original_index
                    scheduled_timestamp = format_utc_milliseconds(
                        schedule_start
                        + timedelta(
                            milliseconds=sequence * plan.scheduled_interval_milliseconds
                        )
                    )
                    replay = build_replay_event(
                        original,
                        replay_run_id=replay_run_id,
                        replay_iteration=replay_iteration,
                        replay_sequence_number=sequence,
                        scheduled_replay_timestamp=scheduled_timestamp,
                        pipeline_version=pipeline_version,
                    )
                    line = (
                        json.dumps(replay, sort_keys=True, separators=(",", ":")).encode(
                            "utf-8"
                        )
                        + b"\n"
                    )
                    output.write(line)
                    output_digest.update(line)
                    output_bytes += len(line)
                    output_count += 1
                    unique_event_ids.add(str(replay["event_id"]))
                    first_scheduled_timestamp = first_scheduled_timestamp or scheduled_timestamp
                    last_scheduled_timestamp = scheduled_timestamp

        if output_count != expected_output_count:
            raise ReplayGenerationError("replay output count does not reconcile")
        if len(unique_event_ids) != expected_output_count:
            raise ReplayGenerationError("replay event IDs are not unique")
        if len(unique_detection_ids) != plan.source_record_count:
            raise ReplayGenerationError("unique detection count does not reconcile")
        temporary_path.replace(events_path)

        duration_seconds = time.perf_counter() - started_clock
        output_sha256 = output_digest.hexdigest()
        manifest = {
            **failure_manifest,
            "status": "SUCCEEDED",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": duration_seconds,
            "throughput_records_per_second": output_count / duration_seconds,
            "source_sha256": plan.source_input_sha256,
            "source_count": len(originals),
            "replay_factor": plan.replay_factor,
            "expected_output_count": expected_output_count,
            "output_count": output_count,
            "unique_event_id_count": len(unique_event_ids),
            "unique_detection_id_count": len(unique_detection_ids),
            "source_type_counts": {NASA_REPLAY: output_count},
            "synthetic_flag_counts": {"false": output_count, "true": 0},
            "first_replay_sequence_number": 0,
            "last_replay_sequence_number": output_count - 1,
            "first_scheduled_replay_timestamp": first_scheduled_timestamp,
            "last_scheduled_replay_timestamp": last_scheduled_timestamp,
            "events_path": events_path.as_posix(),
            "output_bytes": output_bytes,
            "output_sha256": output_sha256,
            "reconciled": True,
        }
        write_json_atomically(manifest_path, manifest)
        return ReplayGenerationResult(
            execution_run_id=execution_run_id,
            replay_run_id=replay_run_id,
            output_count=output_count,
            output_sha256=output_sha256,
            output_bytes=output_bytes,
            events_path=events_path.as_posix(),
            manifest_path=manifest_path.as_posix(),
        )
    except Exception as exc:
        if temporary_path.exists():
            temporary_path.unlink()
        failure_manifest.update(
            {
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "failure_category": type(exc).__name__,
                "failure_message": str(exc),
            }
        )
        write_json_atomically(manifest_path, failure_manifest)
        raise ReplayGenerationError(
            f"Replay generation failed for execution {execution_run_id}: {exc}"
        ) from None
