import unittest
from datetime import datetime, timezone

from pydantic import ValidationError

from eo_event_platform.api.models import BoundingBoxQuery, decode_cursor, encode_cursor


class ApiModelTests(unittest.TestCase):
    def test_cursor_round_trip_and_invalid_cursor(self):
        timestamp = datetime(2026, 8, 1, tzinfo=timezone.utc)
        cursor = encode_cursor(timestamp, "event-1")
        self.assertEqual(decode_cursor(cursor), (timestamp, "event-1"))
        with self.assertRaisesRegex(ValueError, "invalid pagination cursor"):
            decode_cursor("not-valid")

    def test_bbox_rejects_inverted_coordinates_and_long_range(self):
        values = {
            "min_longitude": 10,
            "min_latitude": -1,
            "max_longitude": -10,
            "max_latitude": 1,
            "start_time": "2026-08-01T00:00:00Z",
            "end_time": "2026-08-02T00:00:00Z",
        }
        with self.assertRaisesRegex(ValidationError, "min_longitude"):
            BoundingBoxQuery(**values)
        values["max_longitude"] = 20
        values["end_time"] = "2026-08-09T00:00:01Z"
        with self.assertRaisesRegex(ValidationError, "cannot exceed 7 days"):
            BoundingBoxQuery(**values)
