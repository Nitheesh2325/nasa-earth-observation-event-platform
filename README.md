# NASA Earth Observation Event Intelligence Platform

## Status

The local governed pipeline is verified end to end at one million controlled replay messages through Spark batch, Kafka, Spark Structured Streaming recovery, compact Gold, and PostgreSQL/PostGIS serving.

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
- Current gate: Phase 6 complete; Phase 7 has not started

Local measurements and limitations are recorded in `PERFORMANCE_REPORT.md` and `reports/quality/`. No cloud-deployment claim should be inferred.
