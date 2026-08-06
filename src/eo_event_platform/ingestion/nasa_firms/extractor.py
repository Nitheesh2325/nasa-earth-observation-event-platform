"""Bounded NASA FIRMS extraction with immutable Bronze output and manifests."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Protocol


FIRMS_AREA_API = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
APPROVED_SOURCES = frozenset({"VIIRS_SNPP_SP", "VIIRS_SNPP_NRT"})
REQUIRED_COLUMNS = frozenset(
    {
        "latitude",
        "longitude",
        "bright_ti4",
        "scan",
        "track",
        "acq_date",
        "acq_time",
        "satellite",
        "instrument",
        "confidence",
        "version",
        "bright_ti5",
        "frp",
        "daynight",
    }
)
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})
AREA_PATTERN = re.compile(
    r"^-?(?:\d+(?:\.\d+)?),-?(?:\d+(?:\.\d+)?),"
    r"-?(?:\d+(?:\.\d+)?),-?(?:\d+(?:\.\d+)?)$"
)


class Response(Protocol):
    """Minimal HTTP response interface used by the extractor."""

    status: int
    headers: object

    def read(self) -> bytes: ...

    def __enter__(self) -> "Response": ...

    def __exit__(self, *args: object) -> None: ...


@dataclass(frozen=True)
class ExtractionRequest:
    """A bounded NASA FIRMS Area API request without credentials."""

    source_dataset: str
    area: str
    day_range: int
    start_date: str | None = None

    def validate(self) -> None:
        """Validate approved products and bounded request parameters."""
        if self.source_dataset not in APPROVED_SOURCES:
            raise ValueError(f"Unapproved FIRMS source: {self.source_dataset}")
        if not 1 <= self.day_range <= 5:
            raise ValueError("day_range must be from 1 through 5")
        if self.area != "world":
            if not AREA_PATTERN.fullmatch(self.area):
                raise ValueError("area must be 'world' or west,south,east,north")
            west, south, east, north = (float(value) for value in self.area.split(","))
            if not (-180 <= west < east <= 180):
                raise ValueError("area longitude bounds are invalid")
            if not (-90 <= south < north <= 90):
                raise ValueError("area latitude bounds are invalid")
        if self.start_date is not None:
            date.fromisoformat(self.start_date)


@dataclass(frozen=True)
class ExtractionResult:
    """Safe extraction result containing no credential or credentialed URL."""

    ingestion_run_id: str
    status: str
    record_count: int | None
    byte_count: int | None
    sha256: str | None
    raw_object_path: str | None
    manifest_path: str


class ExtractionError(RuntimeError):
    """Raised when a bounded extraction fails contract validation."""


class FirmsExtractor:
    """Extract one bounded FIRMS CSV response into immutable local Bronze storage."""

    def __init__(
        self,
        *,
        bronze_root: Path,
        manifest_root: Path,
        timeout_seconds: int = 60,
        max_attempts: int = 3,
        opener: Callable[..., Response] = urllib.request.urlopen,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        self.bronze_root = bronze_root
        self.manifest_root = manifest_root
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.opener = opener
        self.sleeper = sleeper

    def extract(
        self,
        request: ExtractionRequest,
        *,
        map_key: str,
        pipeline_version: str,
    ) -> ExtractionResult:
        """Execute an extraction and always write a secret-safe run manifest."""
        request.validate()
        if not map_key.strip():
            raise ValueError("NASA_FIRMS_MAP_KEY is empty")

        started_at = datetime.now(timezone.utc)
        ingestion_run_id = str(uuid.uuid4())
        ingestion_date = started_at.date().isoformat()
        run_directory = (
            self.bronze_root
            / f"source_dataset={request.source_dataset}"
            / f"ingestion_date={ingestion_date}"
            / f"ingestion_run_id={ingestion_run_id}"
        )
        manifest_directory = self.manifest_root / "ingestion" / f"ingestion_date={ingestion_date}"
        manifest_path = manifest_directory / f"{ingestion_run_id}.json"

        manifest: dict[str, object] = {
            "ingestion_run_id": ingestion_run_id,
            "status": "FAILED",
            "source_type": "NASA_ORIGINAL",
            "source_dataset": request.source_dataset,
            "request": asdict(request),
            "requested_at": started_at.isoformat(),
            "pipeline_version": pipeline_version,
            "attempt_count": 0,
            "http_status": None,
            "raw_object_path": None,
            "raw_file_name": None,
            "byte_count": None,
            "sha256": None,
            "record_count": None,
            "source_columns": None,
            "failure_category": None,
        }

        try:
            body, http_status, attempt_count = self._request_csv(request, map_key)
            columns, record_count = validate_firms_csv(body)
            digest = hashlib.sha256(body).hexdigest()
            raw_file_name = "firms_response.csv"
            raw_path = run_directory / raw_file_name
            write_bytes_atomically(raw_path, body)

            manifest.update(
                {
                    "status": "SUCCEEDED",
                    "attempt_count": attempt_count,
                    "http_status": http_status,
                    "raw_object_path": raw_path.as_posix(),
                    "raw_file_name": raw_file_name,
                    "byte_count": len(body),
                    "sha256": digest,
                    "record_count": record_count,
                    "source_columns": columns,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            write_json_atomically(manifest_path, manifest)
            return ExtractionResult(
                ingestion_run_id=ingestion_run_id,
                status="SUCCEEDED",
                record_count=record_count,
                byte_count=len(body),
                sha256=digest,
                raw_object_path=raw_path.as_posix(),
                manifest_path=manifest_path.as_posix(),
            )
        except Exception as exc:
            manifest.update(
                {
                    "failure_category": type(exc).__name__,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            write_json_atomically(manifest_path, manifest)
            raise ExtractionError(
                f"FIRMS extraction failed for run {ingestion_run_id}; "
                f"see manifest {manifest_path.as_posix()}"
            ) from None

    def _request_csv(
        self, request: ExtractionRequest, map_key: str
    ) -> tuple[bytes, int, int]:
        url = build_area_url(request, map_key)
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                http_request = urllib.request.Request(
                    url,
                    headers={"User-Agent": "nasa-eo-event-platform/0.1"},
                    method="GET",
                )
                with self.opener(http_request, timeout=self.timeout_seconds) as response:
                    status = int(response.status)
                    if status != 200:
                        raise ExtractionError(f"Unexpected HTTP status {status}")
                    body = response.read()
                    if not body:
                        raise ExtractionError("NASA FIRMS returned an empty response")
                    return body, status, attempt
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code not in RETRYABLE_HTTP_CODES or attempt == self.max_attempts:
                    break
            except (urllib.error.URLError, TimeoutError) as exc:
                last_error = exc
                if attempt == self.max_attempts:
                    break
            if attempt < self.max_attempts:
                self.sleeper(float(2 ** (attempt - 1)))

        if isinstance(last_error, urllib.error.HTTPError):
            raise ExtractionError(
                f"NASA FIRMS HTTP request failed with status {last_error.code}"
            ) from None
        raise ExtractionError("NASA FIRMS request failed after bounded retries") from None


def build_area_url(request: ExtractionRequest, map_key: str) -> str:
    """Build the credentialed URL; callers must never log the result."""
    parts = [
        FIRMS_AREA_API,
        map_key,
        request.source_dataset,
        request.area,
        str(request.day_range),
    ]
    if request.start_date:
        parts.append(request.start_date)
    return "/".join(parts)


def validate_firms_csv(body: bytes) -> tuple[list[str], int]:
    """Validate CSV encoding, required headers, and readable row structure."""
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ExtractionError("NASA FIRMS response is not valid UTF-8 CSV") from exc

    reader = csv.DictReader(io.StringIO(text, newline=""))
    columns = list(reader.fieldnames or [])
    missing = sorted(REQUIRED_COLUMNS.difference(columns))
    if missing:
        raise ExtractionError(f"NASA FIRMS response is missing required columns: {missing}")

    record_count = 0
    try:
        for row in reader:
            if None in row:
                raise ExtractionError("NASA FIRMS response contains a malformed CSV row")
            record_count += 1
    except csv.Error as exc:
        raise ExtractionError("NASA FIRMS response contains malformed CSV") from exc
    return columns, record_count


def load_map_key(env_file: Path) -> str:
    """Load the FIRMS key from the process environment or a local env file."""
    environment_value = os.environ.get("NASA_FIRMS_MAP_KEY", "").strip()
    if environment_value:
        return environment_value
    if not env_file.exists():
        raise ValueError(f"Environment file does not exist: {env_file}")
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name.strip() == "NASA_FIRMS_MAP_KEY":
            cleaned = value.strip().strip('"').strip("'")
            if cleaned:
                return cleaned
    raise ValueError("NASA_FIRMS_MAP_KEY is not configured")


def write_bytes_atomically(path: Path, content: bytes) -> None:
    """Write bytes without exposing a partially completed final object."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".part")
    temporary_path.write_bytes(content)
    temporary_path.replace(path)


def write_json_atomically(path: Path, content: dict[str, object]) -> None:
    """Write deterministic JSON without a partially completed final manifest."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".part")
    temporary_path.write_text(
        json.dumps(content, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary_path.replace(path)
