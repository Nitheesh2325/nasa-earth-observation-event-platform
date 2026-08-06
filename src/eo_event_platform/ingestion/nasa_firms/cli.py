"""Command-line entry point for one bounded NASA FIRMS extraction."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from .extractor import ExtractionRequest, FirmsExtractor, load_map_key


def build_parser() -> argparse.ArgumentParser:
    """Create the bounded extraction command-line interface."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=("VIIRS_SNPP_SP", "VIIRS_SNPP_NRT"),
        required=True,
    )
    parser.add_argument(
        "--area",
        required=True,
        help="world or west,south,east,north",
    )
    parser.add_argument("--day-range", type=int, choices=range(1, 6), required=True)
    parser.add_argument("--date", dest="start_date")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=Path("data/local/bronze/nasa_firms"),
    )
    parser.add_argument(
        "--manifest-root",
        type=Path,
        default=Path("data/local/manifests"),
    )
    return parser


def detect_pipeline_version(repository_root: Path | None = None) -> str:
    """Return a Git revision without requiring the Git executable on PATH."""
    configured = os.environ.get("PIPELINE_VERSION", "").strip()
    if configured:
        return configured

    root = repository_root or Path.cwd()
    head_path = root / ".git" / "HEAD"
    try:
        head_value = head_path.read_text(encoding="utf-8").strip()
        if not head_value.startswith("ref: "):
            return head_value[:12]
        reference = head_value.removeprefix("ref: ")
        reference_path = root / ".git" / reference
        if reference_path.exists():
            return reference_path.read_text(encoding="utf-8").strip()[:12]
        packed_refs_path = root / ".git" / "packed-refs"
        if packed_refs_path.exists():
            for line in packed_refs_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("#") or line.startswith("^"):
                    continue
                revision, packed_reference = line.split(" ", 1)
                if packed_reference == reference:
                    return revision[:12]
    except (OSError, ValueError):
        pass
    return "unversioned"


def main() -> int:
    """Run one extraction and print only non-secret run metadata."""
    args = build_parser().parse_args()
    request = ExtractionRequest(
        source_dataset=args.source,
        area=args.area,
        day_range=args.day_range,
        start_date=args.start_date,
    )
    extractor = FirmsExtractor(
        bronze_root=args.bronze_root,
        manifest_root=args.manifest_root,
    )
    result = extractor.extract(
        request,
        map_key=load_map_key(args.env_file),
        pipeline_version=detect_pipeline_version(),
    )
    print(f"ingestion_run_id={result.ingestion_run_id}")
    print(f"status={result.status}")
    print(f"record_count={result.record_count}")
    print(f"byte_count={result.byte_count}")
    print(f"raw_object_path={result.raw_object_path}")
    print(f"manifest_path={result.manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
