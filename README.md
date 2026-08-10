# NASA Earth Observation Event Intelligence Platform

## Status

The local governed pipeline is verified end to end at one million controlled replay messages through Spark batch, Kafka, Spark Structured Streaming recovery, compact Gold, and PostgreSQL/PostGIS serving. Phase 7 adds a verified Airflow orchestration and operational-metadata boundary around the approved batch vertical slice.

## Mission

Build a professional batch and streaming data-engineering platform for approximately 10 million NASA-derived Earth Observation event messages.

The final workload will contain a documented mixture of:

- Original NASA records
- Enriched records
- Controlled replay events
- Explicitly labeled synthetic scale-test records

## Planned flow

Official NASA source -> Python extraction -> Bronze storage -> controlled replay and synthetic generation -> Kafka -> Spark validation, deduplication, and enrichment -> Silver Parquet -> Gold aggregates -> PostgreSQL/PostGIS -> API and dashboard.

## Engineering status

- Architecture: implemented locally through governed Gold
- Repository foundation: complete
- NASA extraction: bounded 21-record smoke test passed
- Canonical events: 21 accepted, 0 rejected, 0 duplicate; deterministic rerun passed
- Local vertical slice: passed through PostgreSQL/PostGIS at 100,000 replay messages
- AWS deployment: not authorized
- Largest verified official NASA selection: 10,000 original NASA detections
- Largest verified replay processing gate: 1,000,000 messages representing 10,000 NASA detections exactly 100 times each
- Largest completed serving gate: 1,000,000 replay messages representing 10,000 NASA detections
- Airflow orchestration: one fixed production-style DAG; bounded integration and same-identity rerun passed
- Current gate: Phase 7 complete; Phase 8 has not started

Local measurements and limitations are recorded in `PERFORMANCE_REPORT.md` and `reports/quality/`. No cloud-deployment claim should be inferred.

## Airflow Phase 7

The only DAG is `nasa_eo_batch_vertical_slice_v1`. It is manually triggered, accepts a reconciled gate size up to one million, permits one active run, and applies bounded retries and explicit timeouts. The fixed order is extraction, canonical transformation, controlled replay, Spark processing, Gold generation, PostgreSQL/PostGIS load, and verification.

Airflow 3.3.0 is installed with `requirements-airflow.txt` and the official Python 3.12 constraints file. Because Apache Airflow does not support native Windows execution, local DAG tests and `dag.test()` run in the official `apache/airflow:3.3.0-python3.12` Linux image. Runtime databases, logs, and manifests remain under ignored `data/local/airflow/`.

The `integration` profile is capped at 100 records and verifies orchestration, XCom handoff, metadata, checksums, and rerun behavior only. It is not a NASA, Spark, Gold, or PostgreSQL scale claim. The `local` profile invokes preapproved Phase 6 commands supplied as JSON argument arrays in `ASTRAYAN_<STAGE>_COMMAND` environment variables; secrets are not accepted as DAG parameters or XCom values.
