import unittest
from pathlib import Path

from eo_event_platform.gold.build import sha256_file


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
