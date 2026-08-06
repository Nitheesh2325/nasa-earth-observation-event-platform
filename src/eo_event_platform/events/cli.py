"""Command-line entry point for NASA FIRMS canonical event transformation."""

from __future__ import annotations

import argparse
from pathlib import Path

from eo_event_platform.common.metadata import detect_pipeline_version

from .canonicalization import Canonicalizer


def build_parser() -> argparse.ArgumentParser:
    """Create the bounded canonicalization command-line interface."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument(
        "--canonical-root",
        type=Path,
        default=Path("data/local/bronze/canonical_events"),
    )
    parser.add_argument(
        "--quarantine-root",
        type=Path,
        default=Path("data/local/quarantine/canonicalization"),
    )
    parser.add_argument(
        "--manifest-root",
        type=Path,
        default=Path("data/local/manifests"),
    )
    return parser


def main() -> int:
    """Run one canonicalization and print only safe reconciliation metadata."""
    args = build_parser().parse_args()
    canonicalizer = Canonicalizer(
        canonical_root=args.canonical_root,
        quarantine_root=args.quarantine_root,
        manifest_root=args.manifest_root,
    )
    result = canonicalizer.canonicalize(
        args.source_manifest,
        pipeline_version=detect_pipeline_version(),
    )
    print(f"canonicalization_run_id={result.canonicalization_run_id}")
    print(f"status={result.status}")
    print(f"input_count={result.input_count}")
    print(f"accepted_count={result.accepted_count}")
    print(f"rejected_count={result.rejected_count}")
    print(f"duplicate_count={result.duplicate_count}")
    print(f"events_path={result.events_path}")
    print(f"rejected_path={result.rejected_path}")
    print(f"duplicates_path={result.duplicates_path}")
    print(f"manifest_path={result.manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

