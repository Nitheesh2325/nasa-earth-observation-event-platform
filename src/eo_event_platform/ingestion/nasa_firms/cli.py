"""Command-line entry point for one bounded NASA FIRMS extraction."""

from __future__ import annotations

import argparse
import os
import subprocess
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


def detect_pipeline_version() -> str:
    """Return a Git revision without failing extraction outside a Git checkout."""
    configured = os.environ.get("PIPELINE_VERSION", "").strip()
    if configured:
        return configured
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
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

