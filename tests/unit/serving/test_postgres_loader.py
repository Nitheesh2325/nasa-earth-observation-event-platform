import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from eo_event_platform.serving.postgres_loader import INSERT_EVENT_SQL, iter_payloads, load_manifest


def test_direct_insert_targets_compact_projection() -> None:
    destination_columns = INSERT_EVENT_SQL.split(")\nSELECT", maxsplit=1)[0]

    assert "event_payload" not in destination_columns
    assert "governed_content_hash" in destination_columns


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


class PostgresLoaderTests(unittest.TestCase):
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
