from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from eo_event_platform.events.scale_gate_selection import (
    SelectionError,
    select_scale_gate_events,
)


class ScaleGateSelectionTests(unittest.TestCase):
    def _write_input(self, root: Path, event_ids: list[str]) -> Path:
        events_path = root / "canonical" / "events.jsonl"
        events_path.parent.mkdir(parents=True)
        body = b"".join(
            json.dumps({"event_id": event_id}, separators=(",", ":")).encode()
            + b"\n"
            for event_id in event_ids
        )
        events_path.write_bytes(body)
        manifest_path = root / "canonical-manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "status": "SUCCEEDED",
                    "reconciled": True,
                    "accepted_count": len(event_ids),
                    "events_path": events_path.as_posix(),
                    "events_sha256": hashlib.sha256(body).hexdigest(),
                    "source_type": "NASA_ORIGINAL",
                    "source_dataset": "VIIRS_SNPP_SP",
                    "source_ingestion_run_id": "source-run",
                    "canonicalization_run_id": "canonical-run",
                }
            ),
            encoding="utf-8",
        )
        return manifest_path

    def test_selects_exact_count_in_stable_event_id_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._write_input(root, ["event-c", "event-a", "event-b"])
            result = select_scale_gate_events(
                manifest,
                target_count=2,
                output_root=root / "output",
                manifest_root=root / "manifests",
                pipeline_version="test-revision",
            )
            events = [
                json.loads(line)
                for line in Path(result.events_path).read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(["event-a", "event-b"], [event["event_id"] for event in events])
            self.assertEqual(2, result.selected_count)

    def test_repeated_selection_has_identical_event_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._write_input(root, ["event-b", "event-a"])
            checksums = {
                select_scale_gate_events(
                    manifest,
                    target_count=2,
                    output_root=root / "output",
                    manifest_root=root / "manifests",
                    pipeline_version="test-revision",
                ).events_sha256
                for _ in range(2)
            }
            self.assertEqual(1, len(checksums))

    def test_fails_instead_of_duplicating_when_input_is_too_small(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._write_input(root, ["event-a"])
            with self.assertRaises(SelectionError):
                select_scale_gate_events(
                    manifest,
                    target_count=2,
                    output_root=root / "output",
                    manifest_root=root / "manifests",
                    pipeline_version="test-revision",
                )


if __name__ == "__main__":
    unittest.main()
