"""Tests for canonical event transformation and reconciliation."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from eo_event_platform.events.canonicalization import (
    CanonicalizationError,
    Canonicalizer,
    canonicalize_row,
    sha256_file,
)


HEADER = (
    "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,"
    "satellite,instrument,confidence,version,bright_ti5,frp,daynight\n"
)
VALID_ROW = (
    "34.1000,-118.2000,330.1,0.4,0.5,2026-08-06,42,"
    "N,VIIRS,n,2.0NRT,299.0,3.2,N\n"
)
INVALID_ROW = (
    "999,-118.3000,330.1,0.4,0.5,2026-08-06,55,"
    "N,VIIRS,n,2.0NRT,299.0,3.2,N\n"
)


def write_source_run(root: Path, csv_text: str, record_count: int) -> Path:
    raw_path = root / "bronze" / "firms_response.csv"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_text(csv_text, encoding="utf-8")
    manifest = {
        "status": "SUCCEEDED",
        "source_dataset": "VIIRS_SNPP_NRT",
        "ingestion_run_id": "source-run-1",
        "completed_at": "2026-08-06T21:00:00+00:00",
        "raw_object_path": raw_path.as_posix(),
        "sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(),
        "record_count": record_count,
    }
    manifest_path = root / "manifests" / "source.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


def read_json_lines(path: str) -> list[dict[str, object]]:
    text = Path(path).read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line]


class CanonicalizationTests(unittest.TestCase):
    def test_valid_duplicate_and_rejected_rows_reconcile(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source_manifest_path = write_source_run(
                root, HEADER + VALID_ROW + VALID_ROW + INVALID_ROW, 3
            )
            canonicalizer = Canonicalizer(
                canonical_root=root / "canonical",
                quarantine_root=root / "quarantine",
                manifest_root=root / "run-manifests",
            )

            result = canonicalizer.canonicalize(
                source_manifest_path, pipeline_version="test-revision"
            )

            self.assertEqual(result.input_count, 3)
            self.assertEqual(result.accepted_count, 1)
            self.assertEqual(result.rejected_count, 1)
            self.assertEqual(result.duplicate_count, 1)
            events = read_json_lines(result.events_path)
            rejected = read_json_lines(result.rejected_path)
            duplicates = read_json_lines(result.duplicates_path)
            self.assertEqual(len(events), 1)
            self.assertEqual(len(rejected), 1)
            self.assertEqual(len(duplicates), 1)
            self.assertIn("LATITUDE_OUT_OF_RANGE", rejected[0]["validation_error_codes"])

    def test_accepted_original_event_satisfies_lineage_invariants(self) -> None:
        row = {
            "latitude": "34.1",
            "longitude": "-118.2",
            "bright_ti4": "330.1",
            "scan": "0.4",
            "track": "0.5",
            "acq_date": "2026-08-06",
            "acq_time": "42",
            "satellite": "N",
            "instrument": "VIIRS",
            "confidence": "n",
            "version": "2.0NRT",
            "bright_ti5": "299.0",
            "frp": "3.2",
            "daynight": "N",
        }
        outcome = canonicalize_row(
            row,
            source_dataset="VIIRS_SNPP_NRT",
            ingestion_run_id="source-run-1",
            ingestion_timestamp="2026-08-06T21:00:00Z",
            pipeline_version="test-revision",
            raw_object_uri="raw.csv",
            raw_file_name="raw.csv",
            raw_row_number=2,
        )

        event = outcome.payload
        self.assertEqual(outcome.status, "ACCEPTED")
        self.assertEqual(event["source_type"], "NASA_ORIGINAL")
        self.assertFalse(event["is_synthetic"])
        self.assertEqual(event["event_id"], event["detection_id"])
        self.assertEqual(event["detection_id"], event["lineage_root_id"])
        self.assertEqual(event["source_record_id"], event["event_id"])
        self.assertEqual(event["event_timestamp"], "2026-08-06T00:42:00Z")
        self.assertEqual(event["schema_version"], "1.0.0")

    def test_manifest_hashes_match_all_output_objects(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source_manifest_path = write_source_run(root, HEADER + VALID_ROW, 1)
            result = Canonicalizer(
                canonical_root=root / "canonical",
                quarantine_root=root / "quarantine",
                manifest_root=root / "run-manifests",
            ).canonicalize(source_manifest_path, pipeline_version="test-revision")

            manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
            self.assertTrue(manifest["reconciled"])
            self.assertEqual(manifest["events_sha256"], sha256_file(Path(result.events_path)))
            self.assertEqual(
                manifest["rejected_sha256"], sha256_file(Path(result.rejected_path))
            )
            self.assertEqual(
                manifest["duplicates_sha256"], sha256_file(Path(result.duplicates_path))
            )

    def test_checksum_mismatch_stops_before_outputs_are_written(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source_manifest_path = write_source_run(root, HEADER + VALID_ROW, 1)
            source_manifest = json.loads(
                source_manifest_path.read_text(encoding="utf-8")
            )
            source_manifest["sha256"] = "0" * 64
            source_manifest_path.write_text(json.dumps(source_manifest), encoding="utf-8")
            canonicalizer = Canonicalizer(
                canonical_root=root / "canonical",
                quarantine_root=root / "quarantine",
                manifest_root=root / "run-manifests",
            )

            with self.assertRaisesRegex(CanonicalizationError, "checksum"):
                canonicalizer.canonicalize(
                    source_manifest_path, pipeline_version="test-revision"
                )
            self.assertFalse((root / "canonical").exists())
            failed_manifests = list((root / "run-manifests").rglob("*.json"))
            self.assertEqual(len(failed_manifests), 1)
            failed_manifest = json.loads(
                failed_manifests[0].read_text(encoding="utf-8")
            )
            self.assertEqual(failed_manifest["status"], "FAILED")
            self.assertEqual(
                failed_manifest["failure_category"], "CanonicalizationError"
            )

    def test_manifest_row_count_mismatch_fails_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source_manifest_path = write_source_run(root, HEADER + VALID_ROW, 2)
            canonicalizer = Canonicalizer(
                canonical_root=root / "canonical",
                quarantine_root=root / "quarantine",
                manifest_root=root / "run-manifests",
            )

            with self.assertRaisesRegex(CanonicalizationError, "does not match"):
                canonicalizer.canonicalize(
                    source_manifest_path, pipeline_version="test-revision"
                )


if __name__ == "__main__":
    unittest.main()
