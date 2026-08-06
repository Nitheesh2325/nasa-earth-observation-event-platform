from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from eo_event_platform.replay.generator import ReplayGenerationError, generate_replay
from eo_event_platform.replay.identity import ReplayPlan


def original_event(event_id: str, row_number: int) -> dict[str, object]:
    return {
        "event_id": event_id,
        "detection_id": event_id,
        "lineage_root_id": event_id,
        "source_record_id": event_id,
        "source_type": "NASA_ORIGINAL",
        "source_dataset": "VIIRS_SNPP_SP",
        "is_synthetic": False,
        "ingestion_run_id": "source-run",
        "event_timestamp": "2026-04-01T00:00:00Z",
        "ingestion_timestamp": "2026-08-06T00:00:00Z",
        "latitude": 1.0,
        "longitude": 2.0,
        "raw_object_uri": "raw.csv",
        "raw_row_number": row_number,
        "raw_payload_hash": f"hash-{row_number}",
    }


class ReplayGeneratorTests(unittest.TestCase):
    def _write_source(
        self, root: Path, events: list[dict[str, object]]
    ) -> tuple[Path, str]:
        path = root / "source.jsonl"
        body = b"".join(
            json.dumps(event, sort_keys=True, separators=(",", ":")).encode() + b"\n"
            for event in events
        )
        path.write_bytes(body)
        return path, hashlib.sha256(body).hexdigest()

    def _plan(self, sha256: str, count: int = 2) -> ReplayPlan:
        return ReplayPlan(
            source_input_sha256=sha256,
            source_record_count=count,
            replay_factor=2,
            scheduled_replay_start="2026-08-07T00:00:00.000Z",
            scheduled_interval_milliseconds=10,
        )

    def test_generation_preserves_lineage_and_reconciles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, digest = self._write_source(
                root, [original_event("event-b", 2), original_event("event-a", 1)]
            )
            result = generate_replay(
                source,
                plan=self._plan(digest),
                output_root=root / "output",
                manifest_root=root / "manifests",
                pipeline_version="test-revision",
            )
            events = [
                json.loads(line)
                for line in Path(result.events_path).read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(4, len(events))
            self.assertEqual([0, 1, 2, 3], [e["replay_sequence_number"] for e in events])
            self.assertEqual(["event-a", "event-b", "event-a", "event-b"], [e["parent_event_id"] for e in events])
            self.assertEqual({"NASA_REPLAY"}, {e["source_type"] for e in events})
            self.assertEqual({False}, {e["is_synthetic"] for e in events})
            self.assertEqual({"event-a", "event-b"}, {e["detection_id"] for e in events})
            self.assertEqual(4, len({e["event_id"] for e in events}))
            self.assertEqual("2026-08-07T00:00:00.030Z", events[-1]["scheduled_replay_timestamp"])

    def test_repeat_generation_has_identical_event_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, digest = self._write_source(
                root, [original_event("event-a", 1), original_event("event-b", 2)]
            )
            results = [
                generate_replay(
                    source,
                    plan=self._plan(digest),
                    output_root=root / "output",
                    manifest_root=root / "manifests",
                    pipeline_version="test-revision",
                )
                for _ in range(2)
            ]
            self.assertEqual(results[0].output_sha256, results[1].output_sha256)
            self.assertEqual(
                Path(results[0].events_path).read_bytes(),
                Path(results[1].events_path).read_bytes(),
            )
            self.assertNotEqual(results[0].execution_run_id, results[1].execution_run_id)

    def test_checksum_mismatch_fails_without_events_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, _ = self._write_source(root, [original_event("event-a", 1)])
            plan = self._plan("0" * 64, count=1)
            with self.assertRaises(ReplayGenerationError):
                generate_replay(
                    source,
                    plan=plan,
                    output_root=root / "output",
                    manifest_root=root / "manifests",
                    pipeline_version="test-revision",
                )
            self.assertEqual([], list((root / "output").rglob("events.jsonl")))

    def test_non_original_or_duplicate_source_fails(self) -> None:
        for events in (
            [{**original_event("event-a", 1), "source_type": "NASA_REPLAY"}],
            [original_event("event-a", 1), original_event("event-a", 2)],
        ):
            with self.subTest(events=events):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    source, digest = self._write_source(root, events)
                    with self.assertRaises(ReplayGenerationError):
                        generate_replay(
                            source,
                            plan=self._plan(digest, count=len(events)),
                            output_root=root / "output",
                            manifest_root=root / "manifests",
                            pipeline_version="test-revision",
                        )


if __name__ == "__main__":
    unittest.main()
