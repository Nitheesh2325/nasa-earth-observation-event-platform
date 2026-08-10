# Gold and PostgreSQL Serving Contract

## Purpose

Define the boundary between trusted Silver events, authoritative Gold analytical products, and the rebuildable PostgreSQL/PostGIS serving projection.

## Authority

- Silver is the trusted event-level source.
- Gold Parquet is the authoritative aggregate and serving-product source.
- PostgreSQL/PostGIS is a query-optimized projection and can be rebuilt from governed Gold artifacts.
- Dashboard or API output must never be the source for upstream data.

## Required Gold Products

| Product | Grain | Required truth distinction |
|---|---|---|
| `event_detail` | one accepted unique `event_id` | source type and synthetic flag |
| `event_hourly_spatial` | activity hour, spatial scheme, spatial cell, dataset, source type | original, replay, and synthetic message counts |
| `event_daily_region` | event date, region, dataset, source type | event messages and unique detections |
| `detection_lineage_summary` | one lineage root | original-message, replay-message, and synthetic-message counts |
| `dataset_daily_summary` | event date, dataset, source type | volume and quality totals |
| `platform_summary` | one Gold run | complete reconciliation and scale disclosure |

## Mandatory Fields in Every Aggregate

- `gold_run_id`
- `gold_contract_version`
- `pipeline_version`
- grain columns
- `event_message_count`
- `unique_event_count`
- `unique_detection_count`
- `original_message_count`
- `replay_message_count`
- `synthetic_message_count`
- `source_silver_run_ids`
- `generated_at`

Counts must never imply that replay or synthetic messages are distinct original NASA observations.

## Event Detail Rules

- Contains only accepted, unique Silver events.
- Preserves canonical identity, classification, measurements, time, and lineage.
- Preserves the Silver processing and Kafka lineage needed for audit.
- Adds a deterministic governed content hash for database conflict detection.
- Converts coordinates to PostGIS geometry only in the database projection; longitude is X, latitude is Y, and SRID is 4326.
- Does not expose raw lineage fields through public API responses by default.

## Activity Time

`event_timestamp` remains the observation time. Replay-oriented throughput and map-animation products use `scheduled_replay_timestamp` as activity time for `NASA_REPLAY`; other source types use `event_timestamp`. The selected time meaning must be explicit in every product and API response.

## Physical Gold Format

- Parquet with an explicit schema.
- Compact target files rather than direct consumption of streaming small files.
- The database load boundary may contain multiple newline-delimited JSON part files so scale gates do not require one multi-gigabyte artifact.
- Versioned product and run prefixes.
- Immutable manifest containing object URI, size, SHA-256, row count, schema version, pipeline version, upstream runs, and creation time.
- Every database load part records its own non-negative row count; the sum of all declared part rows must equal the manifest's expected row count before PostgreSQL staging begins.
- A version 1.0 manifest with exactly one load part and no per-part row field remains readable; partitioned manifests require row counts on every load part.
- Generated artifacts remain outside Git; compact contracts and evidence are committed.

## Reconciliation

For an event-detail Gold run:

```text
accepted_silver_rows = gold_event_detail_rows
gold_event_detail_distinct_event_ids = gold_event_detail_rows
original + replay + synthetic = gold_event_detail_rows
is_synthetic_true = synthetic
```

Every aggregate must reconcile to its declared source detail population. Empty dimension values are represented by a governed unknown value rather than silently dropping rows from grouped totals.

## PostgreSQL Projection Rules

- Load only checksum-admitted Gold artifacts.
- Use bulk loading through isolated staging.
- Validate before modifying serving tables.
- Commit serving changes and load-control success atomically.
- Treat a matching existing event as an idempotent no-op.
- Treat a differing content hash for an existing event ID as a hard failure.
- Never update immutable event identity or lineage in place.
- Record database read-back counts and relation sizes after load.

## Compatibility

Adding an optional product field may be backward compatible. Removing or renaming a field, changing grain, changing time meaning, changing count semantics, changing identity/content-hash logic, or changing a PostgreSQL constraint is breaking and requires a versioned contract plus migration plan.
