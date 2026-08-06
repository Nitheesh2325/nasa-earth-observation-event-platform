"""Tests for bounded NASA FIRMS extraction."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import traceback
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from eo_event_platform.ingestion.nasa_firms.extractor import (
    ExtractionError,
    ExtractionRequest,
    FirmsExtractor,
    load_map_key,
)


VALID_CSV = b"""latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_ti5,frp,daynight\n34.1,-118.2,330.1,0.4,0.5,2026-08-06,0042,N,VIIRS,n,2.0NRT,299.0,3.2,N\n35.2,-119.3,340.1,0.5,0.6,2026-08-06,0142,N,VIIRS,h,2.0NRT,300.0,4.2,N\n"""


class FakeResponse:
    """Minimal in-memory HTTP response."""

    def __init__(self, body: bytes, status: int = 200) -> None:
        self.body = body
        self.status = status
        self.headers: dict[str, str] = {}

    def read(self) -> bytes:
        return self.body

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class ExtractionRequestTests(unittest.TestCase):
    def test_rejects_unapproved_source(self) -> None:
        request = ExtractionRequest("MODIS_NRT", "world", 1)
        with self.assertRaisesRegex(ValueError, "Unapproved"):
            request.validate()

    def test_rejects_invalid_bounding_box(self) -> None:
        request = ExtractionRequest("VIIRS_SNPP_NRT", "-100,40,-110,30", 1)
        with self.assertRaisesRegex(ValueError, "longitude"):
            request.validate()

    def test_accepts_bounded_request(self) -> None:
        request = ExtractionRequest(
            "VIIRS_SNPP_NRT", "-125,32,-114,42", 1, "2026-08-06"
        )
        request.validate()


class FirmsExtractorTests(unittest.TestCase):
    def test_success_writes_raw_bytes_and_secret_safe_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            extractor = FirmsExtractor(
                bronze_root=root / "bronze",
                manifest_root=root / "manifests",
                opener=lambda *_args, **_kwargs: FakeResponse(VALID_CSV),
                sleeper=lambda _seconds: None,
            )
            result = extractor.extract(
                ExtractionRequest("VIIRS_SNPP_NRT", "-125,32,-114,42", 1),
                map_key="super-secret-test-key",
                pipeline_version="test-version",
            )

            self.assertEqual(result.status, "SUCCEEDED")
            self.assertEqual(result.record_count, 2)
            self.assertEqual(result.byte_count, len(VALID_CSV))
            self.assertEqual(result.sha256, hashlib.sha256(VALID_CSV).hexdigest())
            self.assertEqual(Path(result.raw_object_path or "").read_bytes(), VALID_CSV)

            manifest_text = Path(result.manifest_path).read_text(encoding="utf-8")
            manifest = json.loads(manifest_text)
            self.assertNotIn("super-secret-test-key", manifest_text)
            self.assertEqual(manifest["record_count"], 2)
            self.assertEqual(manifest["source_type"], "NASA_ORIGINAL")

    def test_contract_failure_writes_failed_manifest_without_raw_object(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            extractor = FirmsExtractor(
                bronze_root=root / "bronze",
                manifest_root=root / "manifests",
                opener=lambda *_args, **_kwargs: FakeResponse(b"wrong,columns\n1,2\n"),
                sleeper=lambda _seconds: None,
            )

            with self.assertRaises(ExtractionError):
                extractor.extract(
                    ExtractionRequest("VIIRS_SNPP_NRT", "world", 1),
                    map_key="super-secret-test-key",
                    pipeline_version="test-version",
                )

            manifests = list((root / "manifests").rglob("*.json"))
            self.assertEqual(len(manifests), 1)
            manifest_text = manifests[0].read_text(encoding="utf-8")
            manifest = json.loads(manifest_text)
            self.assertEqual(manifest["status"], "FAILED")
            self.assertNotIn("super-secret-test-key", manifest_text)
            self.assertEqual(list((root / "bronze").rglob("*.csv")), [])

    def test_transient_failure_is_retried_and_attempt_count_is_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            call_count = 0
            sleep_intervals: list[float] = []

            def flaky_opener(*_args: object, **_kwargs: object) -> FakeResponse:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise urllib.error.URLError("temporary failure")
                return FakeResponse(VALID_CSV)

            extractor = FirmsExtractor(
                bronze_root=root / "bronze",
                manifest_root=root / "manifests",
                opener=flaky_opener,
                sleeper=sleep_intervals.append,
            )
            result = extractor.extract(
                ExtractionRequest("VIIRS_SNPP_NRT", "-125,32,-114,42", 1),
                map_key="super-secret-test-key",
                pipeline_version="test-version",
            )

            manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
            self.assertEqual(call_count, 2)
            self.assertEqual(sleep_intervals, [1.0])
            self.assertEqual(manifest["attempt_count"], 2)

    def test_empty_response_fails_without_creating_raw_object(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            extractor = FirmsExtractor(
                bronze_root=root / "bronze",
                manifest_root=root / "manifests",
                opener=lambda *_args, **_kwargs: FakeResponse(b""),
                sleeper=lambda _seconds: None,
            )

            with self.assertRaises(ExtractionError):
                extractor.extract(
                    ExtractionRequest("VIIRS_SNPP_NRT", "world", 1),
                    map_key="super-secret-test-key",
                    pipeline_version="test-version",
                )

            self.assertEqual(list((root / "bronze").rglob("*.csv")), [])

    def test_http_failure_traceback_does_not_expose_map_key(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            secret = "super-secret-test-key"

            def failing_opener(*_args: object, **_kwargs: object) -> FakeResponse:
                raise urllib.error.HTTPError(
                    url=f"https://example.invalid/{secret}/request",
                    code=403,
                    msg="Forbidden",
                    hdrs=None,
                    fp=None,
                )

            extractor = FirmsExtractor(
                bronze_root=root / "bronze",
                manifest_root=root / "manifests",
                max_attempts=1,
                opener=failing_opener,
                sleeper=lambda _seconds: None,
            )

            try:
                extractor.extract(
                    ExtractionRequest("VIIRS_SNPP_NRT", "world", 1),
                    map_key=secret,
                    pipeline_version="test-version",
                )
            except ExtractionError as exc:
                rendered_traceback = "".join(
                    traceback.format_exception(type(exc), exc, exc.__traceback__)
                )
            else:
                self.fail("Expected ExtractionError")

            self.assertNotIn(secret, rendered_traceback)


class MapKeyTests(unittest.TestCase):
    def test_loads_key_from_env_file_without_modifying_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            env_file = Path(temporary_directory) / ".env"
            env_file.write_text("NASA_FIRMS_MAP_KEY=test-key\n", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(load_map_key(env_file), "test-key")


if __name__ == "__main__":
    unittest.main()
