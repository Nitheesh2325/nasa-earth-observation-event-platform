import unittest
from datetime import datetime, timezone

from eo_event_platform.spark.batch.verify_silver import build_parser, utc_milliseconds


class SilverVerifierTests(unittest.TestCase):
    def test_formats_spark_naive_and_aware_utc_timestamps(self) -> None:
        expected = "2026-08-08T00:00:00.000Z"
        self.assertEqual(expected, utc_milliseconds(datetime(2026, 8, 8)))
        self.assertEqual(expected, utc_milliseconds(datetime(2026, 8, 8, tzinfo=timezone.utc)))

    def test_requires_explicit_truth_expectations(self) -> None:
        args = build_parser().parse_args([
            "--silver-path", "silver",
            "--profile", "streaming",
            "--expected-rows", "1000000",
            "--expected-detections", "10000",
            "--expected-replay-factor", "100",
            "--expected-source-type", "NASA_REPLAY",
            "--expected-synthetic", "0",
            "--expected-first-scheduled", "2026-08-08T00:00:00.000Z",
            "--expected-last-scheduled", "2026-08-08T02:46:39.990Z",
        ])

        self.assertEqual(1_000_000, args.expected_rows)
        self.assertEqual("NASA_REPLAY", args.expected_source_type)
        self.assertEqual(0, args.expected_synthetic)
        self.assertEqual("streaming", args.profile)
