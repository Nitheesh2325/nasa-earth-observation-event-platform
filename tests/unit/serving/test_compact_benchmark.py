import unittest

from eo_event_platform.serving.compact_benchmark import plan_summary


class CompactBenchmarkTests(unittest.TestCase):
    def test_plan_summary_preserves_nested_access_path(self) -> None:
        plan = {
            "Execution Time": 1.5,
            "Plan": {
                "Node Type": "Aggregate",
                "Shared Hit Blocks": 3,
                "Plans": [{"Node Type": "Index Only Scan"}],
            },
        }
        self.assertEqual(
            {
                "execution_time_ms": 1.5,
                "node_types": ["Aggregate", "Index Only Scan"],
                "shared_hit_blocks": 3,
                "shared_read_blocks": 0,
            },
            plan_summary(plan),
        )
