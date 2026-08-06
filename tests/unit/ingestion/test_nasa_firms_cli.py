"""Tests for NASA FIRMS command-line support behavior."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from eo_event_platform.ingestion.nasa_firms.cli import detect_pipeline_version


class PipelineVersionTests(unittest.TestCase):
    def test_reads_revision_from_git_metadata_without_git_executable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            reference_path = root / ".git" / "refs" / "heads" / "main"
            reference_path.parent.mkdir(parents=True)
            (root / ".git" / "HEAD").write_text(
                "ref: refs/heads/main\n", encoding="utf-8"
            )
            reference_path.write_text(
                "1234567890abcdef1234567890abcdef12345678\n", encoding="utf-8"
            )

            with patch.dict("os.environ", {}, clear=True):
                self.assertEqual(detect_pipeline_version(root), "1234567890ab")

    def test_environment_override_takes_precedence(self) -> None:
        with patch.dict("os.environ", {"PIPELINE_VERSION": "release-1"}, clear=True):
            self.assertEqual(detect_pipeline_version(Path("missing")), "release-1")


if __name__ == "__main__":
    unittest.main()
