"""Transform raw NASA FIRMS rows into canonical version 1 event messages."""

from __future__ import annotations

import csv
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Mapping

from .identity import build_source_record_id, hash_raw_row, normalize_acquisition_time


SCHEMA_VERSION = "1.0.0"
NASA_ORIGINAL = "NASA_ORIGINAL"


class CanonicalizationError(RuntimeError):
    """Raised when a complete canonicalization run cannot be trusted."""


@dataclass(frozen=True)
class CanonicalizationResult:
    """Paths and reconciled counts from one canonicalization run."""

    canonicalization_run_id: str
    status: str
    input_count: int
    accepted_count: int
    rejected_count: int
    duplicate_count: int
    events_path: str
    rejected_path: str
    duplicates_path: str
    manifest_path: str


@dataclass(frozen=True)
class RowOutcome:
    """One mutually exclusive result for a raw source row."""

    status: str
    payload: dict[str, object]


class Canonicalizer:
    """Canonicalize one verified NASA extraction manifest and raw CSV object."""

    def __init__(
        self,
        *,
        canonical_root: Path,
        quarantine_root: Path,
        manifest_root: Path,
    ) -> None:
        self.canonical_root = canonical_root
        self.quarantine_root = quarantine_root
        self.manifest_root = manifest_root

    def canonicalize(self, source_manifest_path: Path, *, pipeline_version: str) -> CanonicalizationResult:
        """Run canonicalization and always leave a success or failure manifest."""
        started_at = datetime.now(timezone.utc)
        run_id = str(uuid.uuid4())
        manifest_path = (
            self.manifest_root
            / "canonicalization"
            / f"run_date={started_at.date().isoformat()}"
            / f"{run_id}.json"
        )
        try:
            return self._canonicalize(
                source_manifest_path=source_manifest_path,
                pipeline_version=pipeline_version,
                started_at=started_at,
                run_id=run_id,
                manifest_path=manifest_path,
            )
        except Exception as exc:
            failure_manifest = {
                "canonicalization_run_id": run_id,
                "status": "FAILED",
                "schema_version": SCHEMA_VERSION,
                "pipeline_version": pipeline_version,
                "source_manifest_path": source_manifest_path.as_posix(),
                "started_at": started_at.isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "failure_category": type(exc).__name__,
                "failure_message": str(exc),
            }
            write_json_atomically(manifest_path, failure_manifest)
            raise CanonicalizationError(
                f"Canonicalization failed for run {run_id}: {exc}"
            ) from None

    def _canonicalize(
        self,
        *,
        source_manifest_path: Path,
        pipeline_version: str,
        started_at: datetime,
        run_id: str,
        manifest_path: Path,
    ) -> CanonicalizationResult:
        """Validate lineage, transform every row, and reconcile all outcomes."""
        source_manifest = load_and_validate_source_manifest(source_manifest_path)
        raw_path = resolve_raw_path(source_manifest, source_manifest_path)
        validate_raw_checksum(raw_path, str(source_manifest["sha256"]))
        raw_object_uri = str(source_manifest["raw_object_path"])
        ingestion_timestamp = normalize_utc_timestamp(str(source_manifest["completed_at"]))
        ingestion_date = ingestion_timestamp[:10]

        output_directory = (
            self.canonical_root
            / f"source_type={NASA_ORIGINAL}"
            / f"ingestion_date={ingestion_date}"
            / f"canonicalization_run_id={run_id}"
        )
        quarantine_directory = (
            self.quarantine_root
            / f"ingestion_date={ingestion_date}"
            / f"canonicalization_run_id={run_id}"
        )
        events_path = output_directory / "events.jsonl"
        rejected_path = quarantine_directory / "rejected.jsonl"
        duplicates_path = quarantine_directory / "duplicates.jsonl"

        accepted: list[dict[str, object]] = []
        rejected: list[dict[str, object]] = []
        duplicates: list[dict[str, object]] = []
        seen_event_ids: set[str] = set()

        source_dataset = str(source_manifest["source_dataset"])
        ingestion_run_id = str(source_manifest["ingestion_run_id"])

        with raw_path.open("r", encoding="utf-8-sig", newline="") as raw_file:
            reader = csv.DictReader(raw_file)
            for raw_row_number, row in enumerate(reader, start=2):
                outcome = canonicalize_row(
                    row,
                    source_dataset=source_dataset,
                    ingestion_run_id=ingestion_run_id,
                    ingestion_timestamp=ingestion_timestamp,
                    pipeline_version=pipeline_version,
                    raw_object_uri=raw_object_uri,
                    raw_file_name=raw_path.name,
                    raw_row_number=raw_row_number,
                )
                if outcome.status == "REJECTED":
                    rejected.append(outcome.payload)
                    continue
                event_id = str(outcome.payload["event_id"])
                if event_id in seen_event_ids:
                    duplicates.append(
                        {
                            "event_id": event_id,
                            "detection_id": outcome.payload["detection_id"],
                            "source_dataset": source_dataset,
                            "ingestion_run_id": ingestion_run_id,
                            "raw_object_uri": raw_object_uri,
                            "raw_row_number": raw_row_number,
                            "raw_payload_hash": outcome.payload["raw_payload_hash"],
                            "deduplication_status": "DUPLICATE",
                        }
                    )
                    continue
                seen_event_ids.add(event_id)
                accepted.append(outcome.payload)

        input_count = len(accepted) + len(rejected) + len(duplicates)
        expected_count = int(source_manifest["record_count"])
        if input_count != expected_count:
            raise CanonicalizationError(
                f"Source manifest count {expected_count} does not match CSV row count {input_count}"
            )

        write_json_lines_atomically(events_path, accepted)
        write_json_lines_atomically(rejected_path, rejected)
        write_json_lines_atomically(duplicates_path, duplicates)

        output_hashes = {
            "events_sha256": sha256_file(events_path),
            "rejected_sha256": sha256_file(rejected_path),
            "duplicates_sha256": sha256_file(duplicates_path),
        }
        manifest = {
            "canonicalization_run_id": run_id,
            "status": "SUCCEEDED",
            "schema_version": SCHEMA_VERSION,
            "pipeline_version": pipeline_version,
            "source_type": NASA_ORIGINAL,
            "source_dataset": source_dataset,
            "source_ingestion_run_id": ingestion_run_id,
            "source_manifest_path": source_manifest_path.as_posix(),
            "raw_object_uri": raw_object_uri,
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "input_count": input_count,
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "duplicate_count": len(duplicates),
            "reconciled": input_count == len(accepted) + len(rejected) + len(duplicates),
            "events_path": events_path.as_posix(),
            "rejected_path": rejected_path.as_posix(),
            "duplicates_path": duplicates_path.as_posix(),
            **output_hashes,
        }
        write_json_atomically(manifest_path, manifest)
        return CanonicalizationResult(
            canonicalization_run_id=run_id,
            status="SUCCEEDED",
            input_count=input_count,
            accepted_count=len(accepted),
            rejected_count=len(rejected),
            duplicate_count=len(duplicates),
            events_path=events_path.as_posix(),
            rejected_path=rejected_path.as_posix(),
            duplicates_path=duplicates_path.as_posix(),
            manifest_path=manifest_path.as_posix(),
        )


def canonicalize_row(
    row: Mapping[str, str],
    *,
    source_dataset: str,
    ingestion_run_id: str,
    ingestion_timestamp: str,
    pipeline_version: str,
    raw_object_uri: str,
    raw_file_name: str,
    raw_row_number: int,
) -> RowOutcome:
    """Return one accepted or rejected result without dropping the raw lineage."""
    errors: list[str] = []
    latitude = parse_required_float(row, "latitude", errors)
    longitude = parse_required_float(row, "longitude", errors)
    if latitude is not None and not -90 <= latitude <= 90:
        errors.append("LATITUDE_OUT_OF_RANGE")
    if longitude is not None and not -180 <= longitude <= 180:
        errors.append("LONGITUDE_OUT_OF_RANGE")

    event_timestamp = parse_event_timestamp(row, errors)
    satellite = required_text(row, "satellite", errors)
    instrument = required_text(row, "instrument", errors)
    confidence = required_text(row, "confidence", errors)
    source_product_version = required_text(row, "version", errors)
    day_night = required_text(row, "daynight", errors)
    if instrument is not None and instrument.upper() != "VIIRS":
        errors.append("UNEXPECTED_INSTRUMENT")
    if day_night is not None and day_night not in {"D", "N"}:
        errors.append("INVALID_DAY_NIGHT")

    bright_ti4 = parse_optional_float(row, "bright_ti4", errors, positive=True)
    bright_ti5 = parse_optional_float(row, "bright_ti5", errors, positive=True)
    fire_radiative_power = parse_optional_float(row, "frp", errors, non_negative=True)
    scan = parse_optional_float(row, "scan", errors, positive=True)
    track = parse_optional_float(row, "track", errors, positive=True)
    raw_payload_hash = hash_raw_row(row)

    if errors:
        return RowOutcome(
            status="REJECTED",
            payload={
                "source_type": NASA_ORIGINAL,
                "source_dataset": source_dataset,
                "ingestion_run_id": ingestion_run_id,
                "raw_object_uri": raw_object_uri,
                "raw_file_name": raw_file_name,
                "raw_row_number": raw_row_number,
                "raw_payload_hash": raw_payload_hash,
                "validation_status": "REJECTED",
                "validation_error_codes": sorted(set(errors)),
                "raw_record": dict(row),
            },
        )

    try:
        source_record_id = build_source_record_id(row, source_dataset)
    except (KeyError, ValueError):
        return RowOutcome(
            status="REJECTED",
            payload={
                "source_type": NASA_ORIGINAL,
                "source_dataset": source_dataset,
                "ingestion_run_id": ingestion_run_id,
                "raw_object_uri": raw_object_uri,
                "raw_file_name": raw_file_name,
                "raw_row_number": raw_row_number,
                "raw_payload_hash": raw_payload_hash,
                "validation_status": "REJECTED",
                "validation_error_codes": ["IDENTITY_CONSTRUCTION_FAILED"],
                "raw_record": dict(row),
            },
        )

    event_id = source_record_id
    return RowOutcome(
        status="ACCEPTED",
        payload={
            "event_id": event_id,
            "detection_id": source_record_id,
            "lineage_root_id": source_record_id,
            "source_type": NASA_ORIGINAL,
            "source_dataset": source_dataset,
            "source_record_id": source_record_id,
            "is_synthetic": False,
            "ingestion_run_id": ingestion_run_id,
            "event_timestamp": event_timestamp,
            "ingestion_timestamp": ingestion_timestamp,
            "latitude": latitude,
            "longitude": longitude,
            "bright_ti4_kelvin": bright_ti4,
            "bright_ti5_kelvin": bright_ti5,
            "fire_radiative_power_mw": fire_radiative_power,
            "scan_km": scan,
            "track_km": track,
            "confidence": confidence,
            "day_night": day_night,
            "satellite": satellite,
            "instrument": instrument,
            "source_product_version": source_product_version,
            "schema_version": SCHEMA_VERSION,
            "pipeline_version": pipeline_version,
            "raw_object_uri": raw_object_uri,
            "raw_file_name": raw_file_name,
            "raw_row_number": raw_row_number,
            "raw_payload_hash": raw_payload_hash,
            "kafka_topic": None,
            "kafka_partition": None,
            "kafka_offset": None,
            "kafka_timestamp": None,
            "validation_status": "ACCEPTED",
            "validation_error_codes": [],
            "deduplication_status": "UNIQUE",
            "enrichment_status": "NOT_STARTED",
        },
    )


def parse_event_timestamp(row: Mapping[str, str], errors: list[str]) -> str | None:
    """Parse NASA acquisition date and HHMM time into an explicit UTC timestamp."""
    raw_date = row.get("acq_date", "").strip()
    raw_time = row.get("acq_time", "").strip()
    try:
        normalized_time = normalize_acquisition_time(raw_time)
        parsed = datetime.strptime(raw_date + normalized_time, "%Y-%m-%d%H%M")
        return parsed.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        errors.append("INVALID_EVENT_TIMESTAMP")
        return None


def required_text(
    row: Mapping[str, str], field: str, errors: list[str]
) -> str | None:
    """Return required trimmed text or add a stable error code."""
    value = row.get(field, "").strip()
    if not value:
        errors.append(f"MISSING_{field.upper()}")
        return None
    return value


def parse_required_float(
    row: Mapping[str, str], field: str, errors: list[str]
) -> float | None:
    """Parse one required finite numeric field."""
    value = row.get(field, "").strip()
    if not value:
        errors.append(f"MISSING_{field.upper()}")
        return None
    try:
        parsed = float(value)
    except ValueError:
        errors.append(f"INVALID_{field.upper()}")
        return None
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        errors.append(f"INVALID_{field.upper()}")
        return None
    return parsed


def parse_optional_float(
    row: Mapping[str, str],
    field: str,
    errors: list[str],
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> float | None:
    """Parse an optional finite numeric measurement with basic domain bounds."""
    value = row.get(field, "").strip()
    if not value:
        return None
    try:
        parsed = float(value)
    except ValueError:
        errors.append(f"INVALID_{field.upper()}")
        return None
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        errors.append(f"INVALID_{field.upper()}")
        return None
    if positive and parsed <= 0:
        errors.append(f"NON_POSITIVE_{field.upper()}")
    if non_negative and parsed < 0:
        errors.append(f"NEGATIVE_{field.upper()}")
    return parsed


def load_and_validate_source_manifest(path: Path) -> dict[str, object]:
    """Load the successful source manifest required for canonicalization."""
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CanonicalizationError(f"Cannot read source manifest: {path}") from exc
    required = {
        "status",
        "source_dataset",
        "ingestion_run_id",
        "completed_at",
        "raw_object_path",
        "sha256",
        "record_count",
    }
    missing = sorted(required.difference(manifest))
    if missing:
        raise CanonicalizationError(f"Source manifest is missing fields: {missing}")
    if manifest["status"] != "SUCCEEDED":
        raise CanonicalizationError("Source manifest status is not SUCCEEDED")
    return manifest


def resolve_raw_path(manifest: Mapping[str, object], manifest_path: Path) -> Path:
    """Resolve the raw path recorded by the extraction manifest."""
    raw_path = Path(str(manifest["raw_object_path"]))
    if not raw_path.is_absolute():
        raw_path = Path.cwd() / raw_path
    if not raw_path.is_file():
        raise CanonicalizationError(
            f"Raw source object referenced by {manifest_path} does not exist"
        )
    return raw_path


def validate_raw_checksum(raw_path: Path, expected_sha256: str) -> None:
    """Stop before processing when immutable Bronze bytes do not match the manifest."""
    actual = sha256_file(raw_path)
    if actual != expected_sha256:
        raise CanonicalizationError("Raw source checksum does not match source manifest")


def normalize_utc_timestamp(value: str) -> str:
    """Normalize an ISO timestamp to UTC with a `Z` suffix."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise CanonicalizationError("Ingestion timestamp must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def write_json_lines_atomically(path: Path, records: list[dict[str, object]]) -> None:
    """Write deterministic JSON Lines without a partial final object."""
    lines = (json.dumps(record, sort_keys=True, separators=(",", ":")) for record in records)
    content = "\n".join(lines)
    if content:
        content += "\n"
    write_text_atomically(path, content)


def write_json_atomically(path: Path, content: dict[str, object]) -> None:
    """Write deterministic manifest JSON atomically."""
    write_text_atomically(path, json.dumps(content, indent=2, sort_keys=True) + "\n")


def write_text_atomically(path: Path, content: str) -> None:
    """Write UTF-8 text without exposing a partial final object."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".part")
    temporary_path.write_text(content, encoding="utf-8")
    temporary_path.replace(path)


def sha256_file(path: Path) -> str:
    """Calculate SHA-256 without loading the full file into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
