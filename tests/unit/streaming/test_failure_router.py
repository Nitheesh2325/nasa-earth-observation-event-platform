from __future__ import annotations

import unittest

from eo_event_platform.streaming.kafka.failure_router import (
    process_with_bounded_attempts,
    validation_errors,
)


def valid_event() -> dict[str, object]:
    return {
        "event_id": "event-1",
        "lineage_root_id": "lineage-1",
        "source_type": "NASA_REPLAY",
        "source_dataset": "VIIRS_SNPP_SP",
        "source_record_id": "lineage-1",
        "is_synthetic": False,
        "ingestion_run_id": "ingestion-1",
        "event_timestamp": "2026-04-01T00:56:00Z",
        "ingestion_timestamp": "2026-08-06T22:46:39Z",
        "latitude": 10.0,
        "longitude": 20.0,
        "schema_version": "1.0.0",
    }


class FailureRouterTests(unittest.TestCase):
    def test_valid_replay_passes_routing_validation(self) -> None:
        self.assertEqual([], validation_errors(valid_event(), "lineage-1"))

    def test_invalid_event_has_stable_rejection_reasons(self) -> None:
        event = valid_event()
        event["event_id"] = None
        event["latitude"] = 91.0
        self.assertEqual(["INVALID_LATITUDE", "MISSING_EVENT_ID"], validation_errors(event, "lineage-1"))

    def test_controlled_failure_exhausts_exact_attempt_count(self) -> None:
        event = valid_event()
        event["_test_fault_mode"] = "EXHAUST_RETRIES"
        self.assertEqual((False, 3, "RuntimeError"), process_with_bounded_attempts(event, 3))


if __name__ == "__main__":
    unittest.main()
