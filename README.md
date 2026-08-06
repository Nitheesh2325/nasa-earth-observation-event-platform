# NASA Earth Observation Event Intelligence Platform

## Status

The controlled repository foundation, bounded NASA FIRMS extraction, and deterministic canonical-event milestone are complete. Distributed processing has not started.

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

- Architecture: proposed
- Repository foundation: complete
- NASA extraction: bounded 21-record smoke test passed
- Canonical events: 21 accepted, 0 rejected, 0 duplicate; deterministic rerun passed
- Local vertical slice: not started
- AWS deployment: not authorized
- Largest verified extraction: 21 original NASA records
- Largest verified canonicalization: 21 original NASA records
- Largest completed scale gate: none yet

No performance, production-readiness, or cloud-deployment claims should be inferred until supported by recorded evidence.
