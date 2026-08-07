from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from eo_event_platform.replay.generator import generate_replay
from eo_event_platform.replay.identity import ReplayPlan
from eo_event_platform.replay.verify import verify_manifest
from tests.unit.replay.test_generator import original_event


class ReplayVerifierTests(unittest.TestCase):
    def test_recomputes_identity_lineage_schedule_and_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            body = b"".join(
                json.dumps(original_event(event_id, index), sort_keys=True, separators=(",", ":")).encode() + b"\n"
                for index, event_id in enumerate(("event-b", "event-a"), start=1)
            )
            source.write_bytes(body)
            result = generate_replay(
                source,
                plan=ReplayPlan(
                    source_input_sha256=hashlib.sha256(body).hexdigest(),
                    source_record_count=2,
                    replay_factor=3,
                    scheduled_replay_start="2026-08-08T00:00:00.000Z",
                    scheduled_interval_milliseconds=10,
                ),
                output_root=root / "output",
                manifest_root=root / "manifests",
                pipeline_version="test-revision",
            )

            evidence = verify_manifest(Path(result.manifest_path))

            self.assertEqual("PASSED", evidence["status"])
            self.assertEqual(6, evidence["output_count"])
            self.assertEqual(2, evidence["unique_detection_id_count"])
            self.assertEqual(3, evidence["events_per_detection"])

    def test_rejects_mutated_replay_field(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.jsonl"
            body = json.dumps(original_event("event-a", 1), sort_keys=True, separators=(",", ":")).encode() + b"\n"
            source.write_bytes(body)
            result = generate_replay(
                source,
                plan=ReplayPlan(hashlib.sha256(body).hexdigest(), 1, 1, "2026-08-08T00:00:00.000Z", 10),
                output_root=root / "output",
                manifest_root=root / "manifests",
                pipeline_version="test-revision",
            )
            events_path = Path(result.events_path)
            event = json.loads(events_path.read_text(encoding="utf-8"))
            event["source_type"] = "NASA_ORIGINAL"
            events_path.write_text(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "invalid source_type"):
                verify_manifest(Path(result.manifest_path))
