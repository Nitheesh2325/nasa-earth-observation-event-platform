"""Run metadata shared by command-line entry points."""

from __future__ import annotations

import os
from pathlib import Path


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

