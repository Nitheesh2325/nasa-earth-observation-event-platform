# NASA Earth Observation Event Intelligence Platform

## Status

The local governed pipeline is verified end to end at one million controlled replay messages through Spark batch, Kafka, Spark Structured Streaming recovery, compact Gold, PostgreSQL/PostGIS serving, Airflow orchestration, and a bounded read-only FastAPI layer with a replaceable aggregate cache.

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
- FastAPI: five GET-only endpoints; one-million-row integration, permissions, GiST plan, and latency gates passed
- Current gate: Phase 8B complete; dashboard work has not started

Local measurements and limitations are recorded in `PERFORMANCE_REPORT.md` and `reports/quality/`. No cloud-deployment claim should be inferred.

## Airflow Phase 7

The only DAG is `nasa_eo_batch_vertical_slice_v1`. It is manually triggered, accepts a reconciled gate size up to one million, permits one active run, and applies bounded retries and explicit timeouts. The fixed order is extraction, canonical transformation, controlled replay, Spark processing, Gold generation, PostgreSQL/PostGIS load, and verification.

Airflow 3.3.0 is installed with `requirements-airflow.txt` and the official Python 3.12 constraints file. Because Apache Airflow does not support native Windows execution, local DAG tests and `dag.test()` run in the official `apache/airflow:3.3.0-python3.12` Linux image. Runtime databases, logs, and manifests remain under ignored `data/local/airflow/`.

The `integration` profile is capped at 100 records and verifies orchestration, XCom handoff, metadata, checksums, and rerun behavior only. It is not a NASA, Spark, Gold, or PostgreSQL scale claim. The `local` profile invokes preapproved Phase 6 commands supplied as JSON argument arrays in `ASTRAYAN_<STAGE>_COMMAND` environment variables; secrets are not accepted as DAG parameters or XCom values.

## FastAPI Phase 8A

The Version 1.0 API exposes only:

- `GET /health/ready`
- `GET /v1/summary`
- `GET /v1/daily`
- `GET /v1/lineages/{lineage_root_id}`
- `GET /v1/events/bbox`

All event activity filters use `coalesce(scheduled_replay_timestamp, event_timestamp)`. Observation timestamps remain separately labeled. Lineage and spatial results use seek cursors with maximum limits of 100 and 500 respectively; daily aggregates are limited to 200 rows, bounding-box activity ranges to seven days, and summary/daily ranges to 31 days.

Set `EO_API_DATABASE_DSN` to the untracked `eo_api_runtime` credential and run `uvicorn eo_event_platform.api.app:app`. The readiness endpoint refuses an owner, superuser, or writable database session. API responses never expose raw object paths, governed hashes, database errors, or credentials.

## API cache Phase 8B

Only validated successful platform-summary and daily-aggregate responses use the replaceable in-process cache. The fixed local policy is a 60-second TTL, 256 entries, 65,536 bytes per entry, 4,194,304 total serialized bytes, and LRU eviction. Deterministic keys contain only canonical validated request parameters. `Cache-Control: no-cache` or `no-store` safely bypasses cache reads and writes. Any cache failure falls through to the unchanged read-only PostgreSQL path; health, lineage, bounding-box detail, invalid requests, and failed operations are never cached.
