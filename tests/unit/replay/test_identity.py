from __future__ import annotations

import unittest

from eo_event_platform.replay.identity import (
    ReplayPlan,
    build_replay_event_id,
    build_replay_run_id,
)


class ReplayIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = ReplayPlan(
            source_input_sha256="a" * 64,
            source_record_count=2,
            replay_factor=3,
            scheduled_replay_start="2026-08-07T00:00:00.000Z",
            scheduled_interval_milliseconds=10,
        )

    def test_equivalent_plans_have_same_replay_run_id(self) -> None:
        self.assertEqual(build_replay_run_id(self.plan), build_replay_run_id(self.plan))

    def test_plan_change_changes_replay_run_id(self) -> None:
        changed = ReplayPlan(**{**self.plan.__dict__, "replay_factor": 4})
        self.assertNotEqual(build_replay_run_id(self.plan), build_replay_run_id(changed))

    def test_parent_or_iteration_changes_event_id(self) -> None:
        replay_run_id = build_replay_run_id(self.plan)
        baseline = build_replay_event_id(
            replay_run_id=replay_run_id,
            parent_event_id="parent-a",
            replay_iteration=1,
        )
        self.assertNotEqual(
            baseline,
            build_replay_event_id(
                replay_run_id=replay_run_id,
                parent_event_id="parent-a",
                replay_iteration=2,
            ),
        )
        self.assertNotEqual(
            baseline,
            build_replay_event_id(
                replay_run_id=replay_run_id,
                parent_event_id="parent-b",
                replay_iteration=1,
            ),
        )


if __name__ == "__main__":
    unittest.main()

