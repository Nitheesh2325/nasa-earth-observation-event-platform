import unittest
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from eo_event_platform.api.app import create_app


NOW = datetime(2026, 8, 1, tzinfo=timezone.utc)
EVENT = {
    "event_id": "event-1",
    "detection_id": "detection-1",
    "lineage_root_id": "detection-1",
    "source_type": "NASA_REPLAY",
    "source_dataset": "VIIRS_SNPP_SP",
    "source_record_id": "source-1",
    "is_synthetic": False,
    "event_timestamp": NOW,
    "activity_timestamp": NOW,
    "scheduled_replay_timestamp": NOW,
    "replay_iteration": 1,
    "replay_sequence_number": 0,
    "parent_event_id": "parent-1",
    "latitude": 10.0,
    "longitude": 20.0,
}


class FakeStatusService:
    def readiness(self):
        return {"status": "ready", "database": "reachable", "database_role": "eo_api_runtime", "read_only": True}

    def status(self):
        return {
            "last_successful_pipeline_run": NOW,
            "latest_airflow_run_id": "manual__test",
            "latest_airflow_status": "SUCCEEDED",
            "latest_manifest_id": "gold-test",
            "latest_manifest_sha256": "a" * 64,
            "latest_gold_version": "1.1",
            "cache_enabled": True,
            "cache_ttl_seconds": 60.0,
            "cache_entries": 0,
            "api_version": "1.0.0",
            "platform_version": "test-revision",
            "data_freshness": NOW,
            "quality_gate_status": "PASSED",
        }

    def summary(self, _query):
        return {
            "event_message_count": 1,
            "unique_event_count": 1,
            "unique_detection_count": 1,
            "original_message_count": 0,
            "replay_message_count": 1,
            "synthetic_message_count": 0,
            "first_observation_time": NOW,
            "last_observation_time": NOW,
            "first_activity_time": NOW,
            "last_activity_time": NOW,
        }

    def daily(self, _query):
        return [{
            "activity_date": NOW.date(), "source_dataset": "VIIRS_SNPP_SP",
            "source_type": "NASA_REPLAY", "is_synthetic": False,
            "event_message_count": 1, "unique_event_count": 1, "unique_detection_count": 1,
        }]

    def lineage(self, _lineage, _page):
        return {
            "summary": {
                "lineage_root_id": "detection-1", "event_message_count": 1,
                "unique_event_count": 1, "unique_detection_count": 1,
                "original_message_count": 0, "replay_message_count": 1,
                "synthetic_message_count": 0, "first_observation_time": NOW,
                "last_activity_time": NOW,
            },
            "events": [EVENT], "next_cursor": None,
        }

    def bbox(self, _query):
        return {
            "type": "FeatureCollection",
            "features": [{"type": "Feature", "id": "event-1", "geometry": {"type": "Point", "coordinates": (20.0, 10.0)}, "properties": EVENT}],
            "next_cursor": None,
        }


class ApiEndpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        service = FakeStatusService()
        cls.client = TestClient(create_app(service, status_service=service))

    def test_required_endpoints_and_truth_fields(self):
        self.assertEqual(self.client.get("/health/ready").status_code, 200)
        summary = self.client.get("/v1/summary").json()
        self.assertEqual(summary["replay_message_count"], 1)
        self.assertEqual(summary["synthetic_message_count"], 0)
        daily = self.client.get("/v1/daily?start_date=2026-08-01&end_date=2026-08-01").json()
        self.assertEqual(daily["time_semantics"], "activity_time")
        lineage = self.client.get("/v1/lineages/detection-1?limit=1").json()
        self.assertEqual(lineage["events"][0]["source_type"], "NASA_REPLAY")
        bbox = self.client.get(
            "/v1/events/bbox?min_longitude=19&min_latitude=9&max_longitude=21&max_latitude=11"
            "&start_time=2026-08-01T00:00:00Z&end_time=2026-08-02T00:00:00Z&limit=1"
        ).json()
        self.assertEqual(bbox["type"], "FeatureCollection")
        self.assertEqual(bbox["features"][0]["geometry"]["coordinates"], [20.0, 10.0])
        status = self.client.get("/v1/platform/status").json()
        self.assertEqual(status["quality_gate_status"], "PASSED")
        self.assertNotIn("manifest_path", status)

    def test_invalid_limits_coordinates_ranges_and_extra_parameters_fail(self):
        self.assertEqual(self.client.get("/v1/lineages/x?limit=101").status_code, 422)
        self.assertEqual(self.client.get("/v1/daily?start_date=2026-08-01&end_date=2026-08-01&limit=201").status_code, 422)
        self.assertEqual(self.client.get("/v1/summary?unknown=value").status_code, 422)
        self.assertEqual(self.client.get("/v1/platform/status?unknown=value").status_code, 422)
        invalid = self.client.get(
            "/v1/events/bbox?min_longitude=30&min_latitude=0&max_longitude=20&max_latitude=10"
            "&start_time=2026-08-01T00:00:00Z&end_time=2026-08-02T00:00:00Z"
        )
        self.assertEqual(invalid.status_code, 422)

    def test_openapi_has_explicit_response_schemas_and_no_write_routes(self):
        schema = self.client.get("/openapi.json").json()
        paths = schema["paths"]
        self.assertEqual(set(paths), {"/health/ready", "/v1/platform/status", "/v1/summary", "/v1/daily", "/v1/lineages/{lineage_root_id}", "/v1/events/bbox"})
        self.assertTrue(all(set(operations) == {"get"} for operations in paths.values()))
        for operations in paths.values():
            self.assertIn("$ref", operations["get"]["responses"]["200"]["content"]["application/json"]["schema"])
