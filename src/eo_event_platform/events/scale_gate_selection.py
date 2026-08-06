"""Deterministically select a governed canonical-event scale-gate input."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from eo_event_platform.ingestion.nasa_firms.extractor import write_json_atomically


SELECTION_ALGORITHM = "event-id-ascending-v1"


@dataclass(frozen=True)
class SelectionResult:
    """Safe metadata for one deterministic scale-gate selection."""

    selection_run_id: str
    selected_count: int
    events_sha256: str
    events_path: str
    manifest_path: str


class SelectionError(RuntimeError):
    """Raised when canonical input cannot produce a valid scale-gate artifact."""


def select_scale_gate_events(
    canonical_manifest_path: Path,
    *,
    target_count: int,
    output_root: Path,
    manifest_root: Path,
    pipeline_version: str,
) -> SelectionResult:
    """Verify, order, and select exactly ``target_count`` unique canonical events."""
    if target_count <= 0:
        raise ValueError("target_count must be positive")

    canonical_manifest = json.loads(
        canonical_manifest_path.read_text(encoding="utf-8")
    )
    if canonical_manifest.get("status") != "SUCCEEDED":
        raise SelectionError("canonicalization manifest is not successful")
    if canonical_manifest.get("reconciled") is not True:
        raise SelectionError("canonicalization manifest is not reconciled")

    events_path = Path(str(canonical_manifest["events_path"]))
    source_bytes = events_path.read_bytes()
    source_sha256 = hashlib.sha256(source_bytes).hexdigest()
    if source_sha256 != canonical_manifest.get("events_sha256"):
        raise SelectionError("canonical event checksum does not match its manifest")

    keyed_lines: list[tuple[str, bytes]] = []
    seen_event_ids: set[str] = set()
    for line_number, raw_line in enumerate(source_bytes.splitlines(), start=1):
        try:
            event = json.loads(raw_line)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise SelectionError(
                f"canonical event line {line_number} is not valid JSON"
            ) from exc
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            raise SelectionError(f"canonical event line {line_number} has no event_id")
        if event_id in seen_event_ids:
            raise SelectionError(f"duplicate canonical event_id found: {event_id}")
        seen_event_ids.add(event_id)
        keyed_lines.append((event_id, raw_line))

    accepted_count = canonical_manifest.get("accepted_count")
    if accepted_count != len(keyed_lines):
        raise SelectionError("canonical accepted count does not match event lines")
    if len(keyed_lines) < target_count:
        raise SelectionError(
            f"only {len(keyed_lines)} unique events are available for target {target_count}"
        )

    keyed_lines.sort(key=lambda item: item[0])
    selected_bytes = b"\n".join(line for _, line in keyed_lines[:target_count]) + b"\n"
    selected_sha256 = hashlib.sha256(selected_bytes).hexdigest()

    started_at = datetime.now(timezone.utc)
    selection_run_id = str(uuid.uuid4())
    run_date = started_at.date().isoformat()
    run_directory = (
        output_root
        / f"gate_count={target_count}"
        / f"selection_run_id={selection_run_id}"
    )
    selected_events_path = run_directory / "events.jsonl"
    selected_events_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = selected_events_path.with_suffix(".jsonl.part")
    temporary_path.write_bytes(selected_bytes)
    temporary_path.replace(selected_events_path)

    selection_manifest_path = (
        manifest_root
        / "scale_gate_selection"
        / f"run_date={run_date}"
        / f"{selection_run_id}.json"
    )
    selection_manifest = {
        "selection_run_id": selection_run_id,
        "status": "SUCCEEDED",
        "selection_algorithm": SELECTION_ALGORITHM,
        "target_count": target_count,
        "pre_selection_count": len(keyed_lines),
        "selected_count": target_count,
        "source_type": canonical_manifest.get("source_type"),
        "source_dataset": canonical_manifest.get("source_dataset"),
        "source_ingestion_run_id": canonical_manifest.get("source_ingestion_run_id"),
        "canonicalization_run_id": canonical_manifest.get("canonicalization_run_id"),
        "canonical_manifest_path": canonical_manifest_path.as_posix(),
        "source_events_path": events_path.as_posix(),
        "source_events_sha256": source_sha256,
        "events_path": selected_events_path.as_posix(),
        "events_sha256": selected_sha256,
        "first_event_id": keyed_lines[0][0],
        "last_event_id": keyed_lines[target_count - 1][0],
        "pipeline_version": pipeline_version,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "reconciled": target_count == len(selected_bytes.splitlines()),
    }
    write_json_atomically(selection_manifest_path, selection_manifest)

    return SelectionResult(
        selection_run_id=selection_run_id,
        selected_count=target_count,
        events_sha256=selected_sha256,
        events_path=selected_events_path.as_posix(),
        manifest_path=selection_manifest_path.as_posix(),
    )

