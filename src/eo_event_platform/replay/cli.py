"""Command-line entry point for deterministic controlled NASA replay."""

from __future__ import annotations

import argparse
from pathlib import Path

from eo_event_platform.common.metadata import detect_pipeline_version

from .generator import generate_replay
from .identity import ReplayPlan


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-path", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--source-count", type=int, required=True)
    parser.add_argument("--replay-factor", type=int, required=True)
    parser.add_argument("--scheduled-start", required=True)
    parser.add_argument("--interval-milliseconds", type=int, required=True)
    parser.add_argument(
        "--output-root", type=Path, default=Path("data/local/bronze/replay_events")
    )
    parser.add_argument(
        "--manifest-root", type=Path, default=Path("data/local/manifests")
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    plan = ReplayPlan(
        source_input_sha256=args.source_sha256,
        source_record_count=args.source_count,
        replay_factor=args.replay_factor,
        scheduled_replay_start=args.scheduled_start,
        scheduled_interval_milliseconds=args.interval_milliseconds,
    )
    result = generate_replay(
        args.source_path,
        plan=plan,
        output_root=args.output_root,
        manifest_root=args.manifest_root,
        pipeline_version=detect_pipeline_version(),
    )
    print(f"execution_run_id={result.execution_run_id}")
    print(f"replay_run_id={result.replay_run_id}")
    print(f"output_count={result.output_count}")
    print(f"output_bytes={result.output_bytes}")
    print(f"output_sha256={result.output_sha256}")
    print(f"events_path={result.events_path}")
    print(f"manifest_path={result.manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

