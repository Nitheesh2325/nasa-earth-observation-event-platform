import inspect
import unittest
from datetime import date, datetime, timedelta, timezone

from streamlit.testing.v1 import AppTest

from eo_event_platform.api.models import (
    DailyAggregateItem, DailyAggregateResponse, GeoJSONFeatureCollection,
    LineageResponse, PlatformStatusResponse, PlatformSummaryResponse, ReadinessResponse,
)
from eo_event_platform.dashboard import app
from eo_event_platform.dashboard.client import DashboardApiError


NOW = datetime(2026, 8, 8, tzinfo=timezone.utc)


def dashboard_test_entry(client):
    from eo_event_platform.dashboard.app import render_dashboard
    render_dashboard(client)


class FakeClient:
    def __init__(self, *, fail=False, empty=False, large=False):
        self.fail = fail
        self.empty = empty
        self.large = large
        self.bbox_called = False
        self.lineage_called = False

    def readiness(self):
        if self.fail: raise DashboardApiError("offline")
        return ReadinessResponse(status="ready", database="reachable", database_role="eo_api_runtime", read_only=True)

    def status(self):
        return PlatformStatusResponse(
            last_successful_pipeline_run=NOW, latest_airflow_run_id="manual__run",
            latest_airflow_status="SUCCEEDED", latest_manifest_id="manifest-id",
            latest_manifest_sha256="a" * 64, latest_gold_version="1.1.0", cache_enabled=True,
            cache_ttl_seconds=60, cache_entries=2, api_version="1.0.0", platform_version="v1",
            data_freshness=NOW, quality_gate_status="PASSED",
        )

    def summary(self):
        return PlatformSummaryResponse(
            event_message_count=1_000_000, unique_event_count=1_000_000, unique_detection_count=10_000,
            original_message_count=0, replay_message_count=1_000_000, synthetic_message_count=0,
            first_observation_time=NOW, last_observation_time=NOW,
            first_activity_time=NOW, last_activity_time=NOW + timedelta(hours=2),
        )

    def daily(self, start_date, end_date, source_type=None):
        if self.empty: return DailyAggregateResponse(items=[])
        count = 200 if self.large else 1
        return DailyAggregateResponse(items=[DailyAggregateItem(
            activity_date=start_date, source_dataset=f"DATASET_{index}", source_type="NASA_REPLAY",
            is_synthetic=False, event_message_count=100, unique_event_count=100, unique_detection_count=10,
        ) for index in range(count)])

    def bbox(self, **_kwargs):
        self.bbox_called = True
        return GeoJSONFeatureCollection(features=[], next_cursor=None)

    def lineage(self, _lineage_id):
        self.lineage_called = True
        return LineageResponse.model_validate({
            "summary": {"lineage_root_id": "root", "event_message_count": 1, "unique_event_count": 1,
                "unique_detection_count": 1, "original_message_count": 0, "replay_message_count": 1,
                "synthetic_message_count": 0, "first_observation_time": NOW, "last_activity_time": NOW},
            "events": [{"event_id": "event", "detection_id": "root", "lineage_root_id": "root",
                "source_type": "NASA_REPLAY", "source_dataset": "VIIRS_SNPP_SP", "source_record_id": "source",
                "is_synthetic": False, "event_timestamp": NOW, "activity_timestamp": NOW,
                "scheduled_replay_timestamp": NOW, "replay_iteration": 1, "replay_sequence_number": 0,
                "parent_event_id": "parent", "latitude": 1, "longitude": 2}], "next_cursor": None,
        })


class DashboardRenderingTests(unittest.TestCase):
    def run_app(self, client):
        return AppTest.from_function(dashboard_test_entry, args=(client,), default_timeout=10).run()

    def test_successful_render_and_large_bounded_daily_dataset(self):
        at = self.run_app(FakeClient(large=True))
        self.assertFalse(at.exception)
        self.assertEqual(at.title, [])
        values = [metric.value for metric in at.metric]
        self.assertIn("1,000,000", values)
        self.assertIn("PASSED", values)
        self.assertTrue(any("200 bounded aggregate rows" in item.value for item in at.caption))

    def test_api_unavailable_renders_error_without_partial_metrics(self):
        at = self.run_app(FakeClient(fail=True))
        self.assertFalse(at.exception)
        self.assertTrue(any("FastAPI serving layer is unavailable" in item.value for item in at.error))
        self.assertEqual(len(at.metric), 0)

    def test_empty_daily_and_initial_map_lineage_states(self):
        at = self.run_app(FakeClient(empty=True))
        messages = [item.value for item in at.info]
        self.assertTrue(any("No activity was recorded" in value for value in messages))
        self.assertTrue(any("bounded area" in value for value in messages))
        self.assertTrue(any("Search for a lineage" in value for value in messages))

    def test_map_filters_submit_bounded_request(self):
        client = FakeClient()
        at = self.run_app(client)
        at.button[0].click().run()
        self.assertFalse(at.exception)
        self.assertTrue(client.bbox_called)
        self.assertTrue(any("No events matched" in item.value for item in at.info))

    def test_lineage_search_renders_chain(self):
        client = FakeClient()
        at = self.run_app(client)
        at.text_input[0].input("root")
        at.button[1].click().run()
        self.assertFalse(at.exception)
        self.assertTrue(client.lineage_called)
        self.assertTrue(at.dataframe)

    def test_all_remote_sections_have_explicit_loading_states(self):
        for function in (app._overview, app._daily, app._map, app._lineage):
            self.assertIn("st.spinner", inspect.getsource(function))


if __name__ == "__main__":
    unittest.main()
