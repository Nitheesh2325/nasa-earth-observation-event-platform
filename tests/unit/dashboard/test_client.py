import unittest
from datetime import date, datetime, timezone

import httpx

from eo_event_platform.dashboard.client import DashboardApiClient, DashboardApiError


NOW = "2026-08-08T00:00:00+00:00"
STATUS = {
    "last_successful_pipeline_run": NOW, "latest_airflow_run_id": "manual__run",
    "latest_airflow_status": "SUCCEEDED", "latest_manifest_id": "manifest",
    "latest_manifest_sha256": "a" * 64, "latest_gold_version": "1.1.0",
    "cache_enabled": True, "cache_ttl_seconds": 60, "cache_entries": 2,
    "api_version": "1.0.0", "platform_version": "v1", "data_freshness": NOW,
    "quality_gate_status": "PASSED",
}


class DashboardClientTests(unittest.TestCase):
    def test_uses_only_approved_routes_and_validates_responses(self):
        seen = []

        def handler(request):
            seen.append(request.url.path)
            if request.url.path == "/health/ready":
                return httpx.Response(200, json={"status": "ready", "database": "reachable", "database_role": "eo_api_runtime", "read_only": True})
            if request.url.path == "/v1/platform/status":
                return httpx.Response(200, json=STATUS)
            if request.url.path == "/v1/summary":
                return httpx.Response(200, json={
                    "time_semantics": "activity_time", "event_message_count": 0, "unique_event_count": 0,
                    "unique_detection_count": 0, "original_message_count": 0, "replay_message_count": 0,
                    "synthetic_message_count": 0, "first_observation_time": None, "last_observation_time": None,
                    "first_activity_time": None, "last_activity_time": None,
                })
            if request.url.path == "/v1/daily":
                return httpx.Response(200, json={"time_semantics": "activity_time", "items": []})
            if request.url.path == "/v1/events/bbox":
                return httpx.Response(200, json={"type": "FeatureCollection", "time_semantics": "activity_time", "features": [], "next_cursor": None})
            if request.url.path == "/v1/lineages/root":
                return httpx.Response(200, json={
                    "time_semantics": "activity_time",
                    "summary": {"lineage_root_id": "root", "event_message_count": 0,
                        "unique_event_count": 0, "unique_detection_count": 0, "original_message_count": 0,
                        "replay_message_count": 0, "synthetic_message_count": 0,
                        "first_observation_time": NOW, "last_activity_time": NOW},
                    "events": [], "next_cursor": None,
                })
            return httpx.Response(404, json={"detail": "not found"})

        client = DashboardApiClient("http://api", transport=httpx.MockTransport(handler))
        client.readiness(); client.status(); client.summary()
        client.daily(date(2026, 8, 8), date(2026, 8, 8))
        client.bbox(
            min_longitude=-1, min_latitude=-1, max_longitude=1, max_latitude=1,
            start_time=datetime(2026, 8, 8, tzinfo=timezone.utc),
            end_time=datetime(2026, 8, 9, tzinfo=timezone.utc), source_type=None, limit=50,
        )
        client.lineage("root")
        self.assertEqual(seen, ["/health/ready", "/v1/platform/status", "/v1/summary", "/v1/daily", "/v1/events/bbox", "/v1/lineages/root"])

    def test_api_unavailable_http_error_and_invalid_payload_are_safe(self):
        cases = (
            lambda _request: (_ for _ in ()).throw(httpx.ConnectError("offline")),
            lambda _request: httpx.Response(503, json={"detail": "unavailable"}),
            lambda _request: httpx.Response(200, json={"unexpected": "payload"}),
        )
        for handler in cases:
            with self.subTest(handler=handler):
                client = DashboardApiClient("http://api", transport=httpx.MockTransport(handler))
                with self.assertRaisesRegex(DashboardApiError, "FastAPI data is unavailable or invalid"):
                    client.status()

    def test_invalid_base_url_and_lineage_parameters_fail_before_http(self):
        with self.assertRaises(ValueError):
            DashboardApiClient("file:///database")
        client = DashboardApiClient("http://api", transport=httpx.MockTransport(lambda _r: httpx.Response(500)))
        with self.assertRaises(ValueError):
            client.lineage("")
        with self.assertRaises(ValueError):
            client.lineage("x" * 257)


if __name__ == "__main__":
    unittest.main()
