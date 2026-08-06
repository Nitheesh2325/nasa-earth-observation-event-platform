# Spark Compatibility Smoke Test

## Result

**Status:** Blocked on native Windows Parquet output

**Date:** 2026-08-06

This is environment-compatibility evidence, not a scale or performance result.

## Installed Runtime

- Python 3.12.10 in the project-local `.venv`
- PySpark 4.0.2
- Py4J 0.10.9.9
- Eclipse Temurin OpenJDK 17.0.20
- Spark 4.0.2
- Scala 2.13.16

The exact PySpark dependency is declared in `pyproject.toml`. The virtual environment remains excluded from Git.

## Checks

| Check | Result |
|---|---|
| Project-local virtual environment created | Pass |
| Pinned PySpark and transitive Py4J installed | Pass |
| Java 17 launcher detected | Pass |
| Spark 4.0.2 launcher started | Pass |
| Spark session and DataFrame creation started | Pass |
| Local Parquet write | Fail |
| Parquet read-back reconciliation | Not run |
| NASA 10,000-record acquisition | Not run |

## Observed Environment Constraints

The native Windows launcher exposed two independent constraints:

1. Spark batch scripts did not safely parse the repository path containing spaces. A temporary no-space drive alias allowed the Spark launcher to start without renaming or moving the repository.
2. Hadoop's Windows local filesystem failed the Parquet write because `HADOOP_HOME` and `winutils.exe` were unavailable. Spark reported the failure while attempting to set local filesystem permissions.

The project will not download an unofficial `winutils.exe` binary. No successful Parquet result is claimed, and the NASA scale-gate acquisition did not proceed.

## Recommended Resolution

Use the Apache Software Foundation's official Linux image `apache/spark:4.0.2-python3` through the already-operational Docker Desktop WSL2 backend.

Benefits:

- exact upstream Spark 4.0.2 parity with the selected EMR Serverless Spark version
- Linux filesystem behavior closer to the final AWS runtime
- no unofficial Windows Hadoop binary
- reproducible container identity that can be pinned by digest
- isolation from Windows launcher path parsing

The image is approximately 741 MB compressed. Pulling it is a new environment dependency and requires owner approval. The first container smoke test must use bounded CPU and memory, mount only the project workspace, write ignored local Parquet data, read it back, reconcile two rows, and exit.

## Gate Decision

Phase 3B is paused at the Spark compatibility gate. Do not acquire the 10,000-record NASA input until the official Linux container passes DataFrame execution and Parquet write/read reconciliation.

