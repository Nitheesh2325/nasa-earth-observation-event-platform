# NASA Earth Observation Event Intelligence Platform

## Status

The controlled repository foundation is complete. The architecture has been designed, but no data pipeline has been implemented or tested yet.

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
- Local vertical slice: not started
- AWS deployment: not authorized
- Largest verified scale: none yet

No performance, production-readiness, or cloud-deployment claims should be inferred until supported by recorded evidence.
