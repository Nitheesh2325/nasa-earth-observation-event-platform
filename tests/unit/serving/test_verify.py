import unittest

from eo_event_platform.serving.verify import expected_truth_counts


class VerifyTruthContractTests(unittest.TestCase):
    def test_replay_gate_distinguishes_messages_from_detections(self) -> None:
        self.assertEqual(
            {
                "rows": 100_000,
                "unique_events": 100_000,
                "unique_detections": 10_000,
                "original": 0,
                "replay": 100_000,
                "synthetic": 0,
                "is_synthetic_true": 0,
            },
            expected_truth_counts(
                rows=100_000,
                unique_detections=10_000,
                original=0,
                replay=100_000,
                synthetic=0,
            ),
        )

    def test_source_type_expectations_must_reconcile(self) -> None:
        with self.assertRaisesRegex(ValueError, "reconcile"):
            expected_truth_counts(
                rows=100,
                unique_detections=10,
                original=0,
                replay=99,
                synthetic=0,
            )
