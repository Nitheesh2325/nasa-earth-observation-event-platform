# Spark Compatibility Smoke Test

## Result

**Status:** Passed in the approved Linux container; native Windows remains unsupported

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

## Approved Linux Container Result

The official image `apache/spark:4.0.2-python3` was pulled and pinned to:

`apache/spark@sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3`

Recorded image size: 777,515,138 bytes. The bounded smoke container used four CPUs, 3 GB maximum memory, Spark driver memory of 2 GB, eight shuffle partitions, and `local[4]`.

| Container check | Result |
|---|---|
| Spark version | 4.0.2 |
| DataFrame rows written | 2 |
| Parquet rows read | 2 |
| Count reconciliation | Pass |
| Container exit code | 0 |

The container uses Python 3.10.12 and JDK 17.0.17. This differs from the Windows development interpreter's Python patch/runtime version, so Python-only contract tests remain part of local verification and the container Spark path must be tested independently.

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

The official Linux container passed DataFrame execution and Parquet write/read reconciliation. Native Windows Spark remains an unsupported development path for file-output jobs. Phase 3B was allowed to continue to bounded NASA acquisition.
