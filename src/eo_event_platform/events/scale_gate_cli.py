"""Command-line entry point for deterministic scale-gate event selection."""

from __future__ import annotations

import argparse
from pathlib import Path

from eo_event_platform.common.metadata import detect_pipeline_version

from .scale_gate_selection import select_scale_gate_events


def build_parser() -> argparse.ArgumentParser:
    """Create the deterministic scale-gate selection interface."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canonical-manifest", type=Path, required=True)
    parser.add_argument("--target-count", type=int, required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/local/bronze/scale_gate_inputs"),
    )
    parser.add_argument(
        "--manifest-root", type=Path, default=Path("data/local/manifests")
    )
    return parser


def main() -> int:
    """Select events and print only safe reconciliation metadata."""
    args = build_parser().parse_args()
    result = select_scale_gate_events(
        args.canonical_manifest,
        target_count=args.target_count,
        output_root=args.output_root,
        manifest_root=args.manifest_root,
        pipeline_version=detect_pipeline_version(),
    )
    print(f"selection_run_id={result.selection_run_id}")
    print(f"selected_count={result.selected_count}")
    print(f"events_sha256={result.events_sha256}")
    print(f"events_path={result.events_path}")
    print(f"manifest_path={result.manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

