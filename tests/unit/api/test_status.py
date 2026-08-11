import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import ValidationError

from eo_event_platform.api.app import create_app
from eo_event_platform.api.cache import BoundedTTLCache
from eo_event_platform.api.models import PlatformStatusResponse
from eo_event_platform.api.status import OperationalMetadataReader, PlatformStatusService


NOW = datetime(2026, 8, 10, 22, 35, tzinfo=timezone.utc)


class StatusRepository:
    def platform_status(self):
        return {
            "latest_manifest_id": "80083cae-e529-462f-bf06-f7576bbc1ecc",
            "latest_manifest_sha256": "b" * 64,
            "latest_gold_version": "1.1",
            "platform_version": "phase6g5",
            "data_freshness": NOW,
            "quality_gate_status": "PASSED",
        }


class FailingStatusService:
    def status(self):
        raise RuntimeError("internal path must not escape")


def write_manifest(root: Path, identity: str, status: str, updated: str, completed: str | None = None):
    path = root / f"orchestration_run_id={identity}" / "manifest.json"
    path.parent.mkdir(parents=True)
    value = {"status": status, "updated_at": updated, "airflow_run_ids": [f"manual__{identity}"]}
    if completed is not None:
        value["completed_at"] = completed
    path.write_text(json.dumps(value), encoding="utf-8")


class PlatformStatusTests(unittest.TestCase):
    def test_service_composes_only_safe_verified_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_manifest(root, "success", "SUCCEEDED", NOW.isoformat(), NOW.isoformat())
            cache = BoundedTTLCache()
            cache.set("private-key", b"private-value", 60)
            result = PlatformStatusService(
                StatusRepository(), OperationalMetadataReader(root), cache, 60.0, "1.0.0"
            ).status()
        validated = PlatformStatusResponse.model_validate(result)
        self.assertEqual(validated.latest_airflow_status, "SUCCEEDED")
        self.assertEqual(validated.cache_entries, 1)
        self.assertNotIn("private-key", result.values())
        self.assertNotIn("private-value", result.values())

    def test_latest_status_and_last_successful_run_are_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_manifest(root, "success", "SUCCEEDED", "2026-08-10T20:00:00+00:00", "2026-08-10T20:00:00+00:00")
            write_manifest(root, "failed", "FAILED", "2026-08-10T21:00:00+00:00")
            result = OperationalMetadataReader(root).latest()
        self.assertEqual(result["latest_airflow_status"], "FAILED")
        self.assertEqual(result["latest_airflow_run_id"], "manual__failed")
        self.assertEqual(result["last_successful_pipeline_run"], "2026-08-10T20:00:00+00:00")

    def test_missing_or_invalid_metadata_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(RuntimeError):
                OperationalMetadataReader(root).latest()
            path = root / "orchestration_run_id=bad" / "manifest.json"
            path.parent.mkdir()
            path.write_text("not-json", encoding="utf-8")
            with self.assertRaises(RuntimeError):
                OperationalMetadataReader(root).latest()

    def test_response_schema_rejects_invalid_hash_and_cache_count(self):
        base = {
            "last_successful_pipeline_run": NOW, "latest_airflow_run_id": "run",
            "latest_airflow_status": "SUCCEEDED", "latest_manifest_id": "manifest",
            "latest_manifest_sha256": "invalid", "latest_gold_version": "1.1",
            "cache_enabled": True, "cache_ttl_seconds": 60, "cache_entries": -1,
            "api_version": "1.0.0", "platform_version": "v1", "data_freshness": NOW,
            "quality_gate_status": "PASSED",
        }
        with self.assertRaises(ValidationError):
            PlatformStatusResponse.model_validate(base)

    def test_endpoint_maps_metadata_failure_to_safe_503(self):
        client = TestClient(create_app(StatusRepository(), status_service=FailingStatusService()))
        response = client.get("/v1/platform/status")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {"detail": "operational metadata unavailable"})


if __name__ == "__main__":
    unittest.main()
