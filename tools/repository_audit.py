"""Fail closed on release-critical repository hygiene using only the standard library."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
MARKDOWN_LINK = re.compile(r"!?\[[^]]*]\(([^)]+)\)")
SECRET_PATTERNS = {
    "AWS access key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "FIRMS key assignment": re.compile(
        "NASA_FIRMS_" + r"MAP_KEY=(?!\s*$|<|replace)[^\s]+"
    ),
}
FORBIDDEN_TRACKED_PREFIXES = ("data/local/", "data/generated/", "data/bronze/", "data/silver/", "data/gold/")
FORBIDDEN_TRACKED_SUFFIXES = (".parquet", ".tfstate", ".pem", ".key")
LINK_DOCUMENTS = {
    "README.md", "ARCHITECTURE.md", "PERFORMANCE_REPORT.md",
    "docs/DATA_DICTIONARY.md", "docs/ENGINEERING_DECISIONS.md",
    "docs/AWS_DESIGN.md", "reports/VERIFICATION_RESULTS.md",
}


def tracked_files() -> list[str]:
    output = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT
    ).decode("utf-8")
    return [item for item in output.split("\0") if item]


def audit_generated_data(paths: list[str]) -> list[str]:
    failures: list[str] = []
    for path in paths:
        normalized = path.replace("\\", "/")
        if normalized.startswith(FORBIDDEN_TRACKED_PREFIXES):
            failures.append(f"generated data is tracked: {path}")
        if normalized.lower().endswith(FORBIDDEN_TRACKED_SUFFIXES):
            failures.append(f"forbidden generated/secret suffix is tracked: {path}")
        absolute = ROOT / path
        if absolute.is_file() and absolute.stat().st_size > 5 * 1024 * 1024:
            failures.append(f"tracked file exceeds 5 MiB release bound: {path}")
    return failures


def audit_secrets(paths: list[str]) -> list[str]:
    failures: list[str] = []
    for path in paths:
        absolute = ROOT / path
        if not absolute.is_file():
            continue
        try:
            text = absolute.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for label, pattern in SECRET_PATTERNS.items():
            for match in pattern.finditer(text):
                if path == "tests/unit/ingestion/test_nasa_firms_extractor.py" and "test-key" in match.group(0):
                    continue
                failures.append(f"possible {label} in {path}")
    return failures


def audit_links() -> list[str]:
    failures: list[str] = []
    for name in sorted(LINK_DOCUMENTS):
        document = ROOT / name
        if not document.exists():
            failures.append(f"required document is missing: {name}")
            continue
        text = document.read_text(encoding="utf-8")
        for raw_target in MARKDOWN_LINK.findall(text):
            target = raw_target.strip().split(maxsplit=1)[0].strip("<>")
            if not target or target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            target = unquote(target.split("#", 1)[0])
            if not (document.parent / target).resolve().exists():
                failures.append(f"broken link in {name}: {raw_target}")
    return failures


def main() -> int:
    paths = tracked_files()
    failures = audit_generated_data(paths) + audit_secrets(paths) + audit_links()
    if failures:
        for failure in failures:
            print(f"ERROR: {failure}")
        return 1
    print(f"Repository audit passed for {len(paths)} release-candidate files.")
    print("Secrets: PASS; generated-data exclusions: PASS; documentation links: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
