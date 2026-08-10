import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from eo_event_platform.serving.postgres_loader import (
    INSERT_EVENT_SQL,
    iter_payloads,
    load_manifest,
    reconcile_idempotent_rows,
)


def _fixture(tmp_path: Path) -> Path:
    load_dir = tmp_path / "load_artifact"
    load_dir.mkdir()
    artifact = load_dir / "part-00000.json"
    artifact.write_text('{"event_id":"one"}\n', encoding="utf-8")
    manifest = {
        "gold_run_id": "00000000-0000-0000-0000-000000000001",
        "gold_contract_version": "1.0.0",
        "pipeline_version": "test",
        "source_silver_path": "silver",
        "expected_rows": 1,
        "load_artifact_rows": 1,
        "artifacts": [{
            "path": "load_artifact/part-00000.json",
            "bytes": artifact.stat().st_size,
            "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
        }],
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def _partitioned_fixture(tmp_path: Path) -> Path:
    load_dir = tmp_path / "load_artifact"
    load_dir.mkdir()
    artifacts = []
    for index in range(2):
        path = load_dir / f"part-{index:05d}.json"
        path.write_text(json.dumps({"event_id": str(index)}) + "\n", encoding="utf-8")
        artifacts.append({
            "path": f"load_artifact/{path.name}",
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "rows": 1,
        })
    manifest = {
        "gold_run_id": "00000000-0000-0000-0000-000000000002",
        "gold_contract_version": "1.1.0",
        "pipeline_version": "test",
        "source_silver_path": "silver",
        "expected_rows": 2,
        "load_artifact_rows": 2,
        "artifacts": artifacts,
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


class PostgresLoaderTests(unittest.TestCase):
    def test_partitioned_load_artifact_rows_reconcile(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest, _ = load_manifest(_partitioned_fixture(Path(directory)), expected_rows=2)
            self.assertEqual(2, sum(item["rows"] for item in manifest["artifacts"]))

    def test_partitioned_load_artifact_row_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = _partitioned_fixture(Path(directory))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["artifacts"][0]["rows"] = 0
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "row counts do not reconcile"):
                load_manifest(manifest_path, expected_rows=2)

    def test_partitioned_load_artifact_requires_all_row_counts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = _partitioned_fixture(Path(directory))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            del manifest["artifacts"][0]["rows"]
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "non-negative integers"):
                load_manifest(manifest_path, expected_rows=2)

    def test_direct_insert_targets_compact_projection(self) -> None:
        destination_columns = INSERT_EVENT_SQL.split(")\nSELECT", maxsplit=1)[0]
        self.assertNotIn("event_payload", destination_columns)
        self.assertIn("governed_content_hash", destination_columns)

    def test_idempotency_requires_persisted_gold_rows(self) -> None:
        reconcile_idempotent_rows(100_000, 100_000)
        with self.assertRaisesRegex(RuntimeError, "successful load metadata exists"):
            reconcile_idempotent_rows(0, 100_000)

    def test_manifest_and_payload_are_admitted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest, key = load_manifest(_fixture(Path(directory)), expected_rows=1)
            self.assertEqual(64, len(key))
            self.assertEqual([('{"event_id":"one"}',)], list(iter_payloads(manifest)))

    def test_manifest_rejects_wrong_gate_count(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "bounded gate"):
                load_manifest(_fixture(Path(directory)), expected_rows=2)

    def test_manifest_rejects_checksum_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = _fixture(root)
            artifact = root / "load_artifact" / "part-00000.json"
            artifact.write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "size mismatch|checksum mismatch"):
                load_manifest(path, expected_rows=1)
