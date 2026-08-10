import unittest
from contextlib import contextmanager
from datetime import datetime, timezone

from eo_event_platform.api.models import BoundingBoxQuery, SummaryQuery
from eo_event_platform.api.repository import ApiRepository


class RecordingResult:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.row


class RecordingConnection:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def execute(self, statement, params=()):
        self.calls.append((statement, params))
        return RecordingResult(self.result)


class RecordingRepository(ApiRepository):
    def __init__(self, connection):
        super().__init__("postgresql://ignored")
        self.recording_connection = connection

    @contextmanager
    def connection(self):
        yield self.recording_connection


class RepositorySqlTests(unittest.TestCase):
    def test_summary_keeps_user_values_out_of_sql(self):
        row = {key: 0 for key in (
            "event_message_count", "unique_event_count", "unique_detection_count",
            "original_message_count", "replay_message_count", "synthetic_message_count",
        )}
        row.update({key: None for key in (
            "first_observation_time", "last_observation_time", "first_activity_time", "last_activity_time",
        )})
        connection = RecordingConnection(row)
        repository = RecordingRepository(connection)
        hostile = "dataset' OR true --"
        repository.summary(SummaryQuery(source_dataset=hostile))
        statement, params = connection.calls[0]
        self.assertNotIn(hostile, statement)
        self.assertEqual(params, (hostile,))
        self.assertIn("source_dataset = %s", statement)

    def test_bbox_plan_uses_gist_compatible_operator_and_parameters(self):
        query = BoundingBoxQuery(
            min_longitude=-10, min_latitude=-5, max_longitude=10, max_latitude=5,
            start_time=datetime(2026, 8, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 8, 2, tzinfo=timezone.utc),
            source_type="NASA_REPLAY", limit=10,
        )
        statement, params = ApiRepository("postgresql://ignored").bbox_sql_for_plan(query)
        self.assertIn("geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)", statement)
        self.assertNotIn("NASA_REPLAY", statement)
        self.assertIn("NASA_REPLAY", params)
        self.assertEqual(params[-1], 10)
