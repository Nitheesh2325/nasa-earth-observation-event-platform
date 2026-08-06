"""Deterministic NASA FIRMS identity rules shared by processing paths."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from typing import Mapping


IDENTITY_VERSION = "nasa-firms-viirs-v1"
IDENTITY_FIELDS = (
    "source_dataset",
    "satellite",
    "acq_date",
    "acq_time",
    "latitude",
    "longitude",
    "version",
)


def normalize_decimal(value: str) -> str:
    """Normalize a finite decimal without binary floating-point conversion."""
    try:
        number = Decimal(value.strip())
    except (InvalidOperation, AttributeError) as exc:
        raise ValueError(f"Invalid decimal value: {value!r}") from exc
    if not number.is_finite():
        raise ValueError(f"Decimal value must be finite: {value!r}")
    if number == 0:
        return "0"
    return format(number.normalize(), "f")


def normalize_acquisition_time(value: str) -> str:
    """Normalize NASA HHMM acquisition time while preserving leading zeros."""
    cleaned = value.strip()
    if not cleaned.isdigit() or not 1 <= len(cleaned) <= 4:
        raise ValueError("Acquisition time must contain one through four digits")
    normalized = cleaned.zfill(4)
    hours = int(normalized[:2])
    minutes = int(normalized[2:])
    if hours > 23 or minutes > 59:
        raise ValueError("Acquisition time is outside valid HHMM bounds")
    return normalized


def build_source_record_id(row: Mapping[str, str], source_dataset: str) -> str:
    """Build the versioned deterministic identity of one NASA source record."""
    identity = {
        "identity_version": IDENTITY_VERSION,
        "source_dataset": source_dataset.strip(),
        "satellite": row["satellite"].strip(),
        "acq_date": row["acq_date"].strip(),
        "acq_time": normalize_acquisition_time(row["acq_time"]),
        "latitude": normalize_decimal(row["latitude"]),
        "longitude": normalize_decimal(row["longitude"]),
        "version": row["version"].strip(),
    }
    serialized = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return f"{IDENTITY_VERSION}:sha256:{digest}"


def hash_raw_row(row: Mapping[str, str]) -> str:
    """Hash a source row using deterministic JSON serialization."""
    serialized = json.dumps(dict(row), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

