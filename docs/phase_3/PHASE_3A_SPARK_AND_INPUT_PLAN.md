# Phase 3A Spark Runtime and 10,000-Record Input Plan

## Status

Approved for research and design. Dependency installation, NASA acquisition, and Spark implementation remain pending owner approval.

## Objective

Prepare a reproducible local Spark environment and a bounded, truthful 10,000-record NASA input for the first Bronze-to-Silver batch scale gate.

## Runtime Decision

Use the following local runtime:

- Python 3.12
- Eclipse Temurin JDK 17
- PySpark 4.0.2
- Spark master `local[4]` for the initial gate

The target AWS runtime is Amazon EMR Serverless release `emr-spark-8.0.0`, which provides Spark 4.0.2, supports Python 3.12, and uses JDK 17 by default. Matching the upstream Spark minor version locally reduces behavioral drift between development and final cloud execution.

PySpark must be pinned exactly to `4.0.2`. A floating latest-version dependency is prohibited because newer upstream Spark releases may not match the selected EMR runtime.

## Minimal Dependency Boundary

The initial approved dependency proposal is limited to:

- `pyspark==4.0.2`
- its required transitive `py4j` dependency

Do not add pandas, PyArrow, NumPy, notebooks, Delta Lake, Iceberg, or testing frameworks during this milestone. The initial transformation uses Spark DataFrame and SQL functions that do not require these optional packages. Any later addition requires a demonstrated use case and a separate decision.

The package must be installed in a project-local virtual environment, never globally. The dependency must be declared and reproducibly locked before implementation begins.

## Local Resource Envelope

The laptop has 16 GB RAM and the workspace is on a mechanical disk. The first batch gate will therefore use conservative defaults:

- four local Spark worker threads
- 2 GB driver memory
- eight shuffle partitions initially
- 256 MB maximum driver result size
- Spark UI enabled during measured runs
- no Dockerized Spark cluster for this gate
- no concurrent Kafka or PostgreSQL containers during the batch measurement

These are starting limits, not unverified performance claims. The measured run will record wall-clock runtime, throughput, input/output counts, rejected rows, duplicate rows, partition counts, and relevant Spark configuration. Configuration changes must be evidence-driven.

## NASA Input Contract

Use the official NASA FIRMS Area API with:

- source: `VIIRS_SNPP_SP`
- area: `world`
- day range: `1`
- date: a fixed historical date confirmed by the FIRMS data-availability endpoint immediately before acquisition
- format: CSV

Standard Processing is selected because it is more stable for a reproducible batch benchmark than near-real-time data. A fixed date prevents the benchmark input from changing according to the day it is run.

NASA documents that a global one-day VIIRS query can return approximately 30,000 to more than 100,000 rows. This makes one bounded request a reasonable source for the 10,000-record gate, but the actual count must be measured and reported rather than assumed.

## Deterministic 10,000-Record Selection

The acquisition workflow will:

1. Check availability for the selected Standard Processing date.
2. Extract one immutable raw global daily response using the existing bounded extractor.
3. Validate the source contract and preserve the raw object, response checksum, and extraction manifest outside Git.
4. Canonicalize every source row using the accepted version 1 event contract.
5. Reconcile accepted, rejected, duplicate, and total counts.
6. Sort accepted unique original NASA events by stable `event_id` in ascending order.
7. Select the first exactly 10,000 events.
8. Write a selection manifest containing the source run ID, canonicalization run ID, selection rule, pre-selection counts, selected count, checksums, pipeline revision, and timestamps.
9. Repeat the selection and verify byte-identical output before admitting it to the Spark gate.

An unordered or nondeterministic Spark `limit(10000)` is prohibited. If fewer than 10,000 valid unique events are available, the gate fails and a different fixed date must be selected and documented; records must not be silently duplicated.

## Truthfulness and Lineage

All selected records remain original NASA-derived observations and retain:

- `source_type`
- `source_dataset`
- `source_record_id`
- `is_synthetic = false`
- `ingestion_run_id`
- `event_timestamp`
- `ingestion_timestamp`

The selection step does not create synthetic or replay events. Later scale gates may use controlled replay or synthetic generation, but those records must be labeled explicitly and reported separately from original NASA observations.

## First Spark Batch Slice

After dependency and acquisition approval, Spark will:

1. Read canonical Bronze JSON Lines with an explicit schema.
2. Quarantine corrupt or contract-invalid rows with reason codes.
3. Validate required fields, coordinates, timestamps, lineage, and source classification.
4. Deduplicate using the stable event key.
5. Add governed processing metadata without overwriting source lineage.
6. Write Silver Parquet partitioned by event date.
7. Emit a machine-readable reconciliation and performance report.
8. Read the written Parquet back and verify counts and schema.

The batch job may advance only when automated tests pass, counts reconcile, quality and runtime metrics are recorded, output is deterministic where required, and limitations are documented.

## Risks and Controls

| Risk | Control |
|---|---|
| PySpark package is large | Install once in a project-local environment and pin the exact version. |
| Windows filesystem or temporary-directory behavior causes failures | Run a minimal local Spark smoke test before building the transformation and document any workaround. |
| Mechanical disk limits shuffle and Parquet performance | Keep partitions bounded, avoid unnecessary shuffles, and treat measured disk performance as a documented laptop limitation. |
| Spark 4 ANSI behavior exposes implicit-cast errors | Use explicit schemas and casts; preserve rejected records instead of silently coercing them. |
| Historical SP availability changes | Check the official availability endpoint and record the selected date and response metadata. |
| Global query exceeds the needed row count | Preserve the complete raw response outside Git, then apply the documented stable selection rule. |
| Global query returns fewer than 10,000 valid unique rows | Fail honestly and select another available fixed date; never manufacture originals. |
| Local and AWS behavior diverge | Pin Spark 4.0.2 locally and target `emr-spark-8.0.0`; run parity checks before final AWS execution. |
| Accidental secret or dataset commit | Keep the map key in `.env`, keep data under ignored local storage, and commit only compact evidence. |

## Approval Boundary

Owner approval is required before:

- creating the project virtual environment
- installing `pyspark==4.0.2`
- changing declared project dependencies
- making the global historical NASA request
- implementing the Spark batch job

## Phase 3A Completion Criteria

Research and design are complete when:

- the local and AWS Spark versions are explicitly aligned
- the minimal dependency boundary is documented
- the deterministic 10,000-record acquisition and selection rules are documented
- laptop risks and starting resource limits are documented
- no dependency or large dataset has been added without approval
- the repository is clean and the decision milestone is preserved in Git

