from __future__ import annotations

import json
import unittest

from eo_event_platform.spark.streaming.kafka_to_silver import (
    WATERMARK_DELAY,
    expected_end_offsets,
    starting_offsets_json,
)


class StreamingContractTests(unittest.TestCase):
    def test_manifest_offsets_become_explicit_spark_offsets(self) -> None:
        manifest = {
            "topic": "eo.events.replay.v1",
            "start_offsets": {"0": 10, "1": 20},
            "end_offsets": {"0": 12, "1": 21},
        }
        self.assertEqual(
            {"eo.events.replay.v1": {"0": 10, "1": 20}},
            json.loads(starting_offsets_json(manifest)),
        )
        self.assertEqual({0: 12, 1: 21}, expected_end_offsets(manifest))

    def test_replay_watermark_is_bounded(self) -> None:
        self.assertEqual("10 minutes", WATERMARK_DELAY)


if __name__ == "__main__":
    unittest.main()
