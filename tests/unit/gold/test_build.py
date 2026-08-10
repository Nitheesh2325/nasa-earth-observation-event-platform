import unittest
from pathlib import Path

from eo_event_platform.gold.build import artifact_entries, artifact_part_count, sha256_file


class GoldBuildTests(unittest.TestCase):
    def test_sha256_file(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "artifact"
            artifact.write_bytes(b"earth-observation")
            self.assertEqual(
                "6dcc043f8d8eb966aafdf1eb541d30697073e732197aa98971847ce54a316343",
                sha256_file(artifact),
            )

    def test_load_artifact_entries_include_exact_row_counts(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            load = root / "load_artifact"
            load.mkdir()
            artifact = load / "part-00000.json"
            artifact.write_bytes(b'{"id":1}\n{"id":2}\n')

            entries = artifact_entries(root)

            self.assertEqual(2, entries[0]["rows"])
            self.assertEqual(18, entries[0]["bytes"])

    def test_artifact_part_count_reports_physical_files(self) -> None:
        artifacts = [
            {"path": "event_detail/_SUCCESS"},
            {"path": "event_detail/part-00000.parquet"},
            {"path": "event_detail/part-00001.parquet"},
            {"path": "load_artifact/part-00000.json"},
        ]

        self.assertEqual(2, artifact_part_count(artifacts, "event_detail/part-", ".parquet"))
        self.assertEqual(1, artifact_part_count(artifacts, "load_artifact/part-", ".json"))
