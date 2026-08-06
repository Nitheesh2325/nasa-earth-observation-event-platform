# Silver Event Data Contract

## Purpose

Silver contains validated, deduplicated, typed canonical events suitable for governed aggregation and downstream serving. It is derived from immutable Bronze input and never replaces Bronze lineage.

## Admission Rules

An event enters Silver only when:

- JSON is readable under the explicit version 1 schema.
- All required identity, lineage, classification, time, and location fields pass validation.
- `source_type` and `is_synthetic` agree.
- Original NASA identity invariants remain intact.
- Measurement values pass the documented domain checks when present.
- The event is the first stable occurrence of its `event_id` within the batch.

Invalid events are written to the rejected quarantine dataset with stable reason codes. Later valid occurrences of an already accepted `event_id` are written to the duplicate quarantine dataset. Rejected and duplicate events never enter trusted Silver.

## Physical Format

- Format: Parquet
- Compression: Spark Parquet default compression
- Partition: `event_date=YYYY-MM-DD`
- Write boundary: unique `spark_processing_run_id`
- Timezone: UTC

High-cardinality identifiers are prohibited as directory partitions.

## Preserved Canonical Fields

Silver preserves the canonical version 1 fields, including:

- event, detection, source-record, and lineage-root identities
- source classification and synthetic flag
- ingestion run and timestamps
- NASA measurements and coordinates
- raw object and row lineage
- Kafka metadata when present
- schema and pipeline versions

## Derived Fields

| Field | Type | Definition |
|---|---|---|
| `event_date` | date | UTC date derived from `event_timestamp`; physical partition key |
| `event_hour` | integer | UTC hour derived from `event_timestamp` |
| `ingestion_date` | date | UTC date derived from `ingestion_timestamp` |
| `geometry_wkt` | string | PostGIS-compatible `POINT (longitude latitude)` representation |
| `processing_timestamp` | timestamp | Fixed UTC timestamp for the Spark processing run |
| `spark_processing_run_id` | string | Unique identity of the Bronze-to-Silver Spark run |
| `spark_validation_status` | string | Spark validation outcome before deduplication |
| `spark_validation_error_codes` | array<string> | Stable Spark reason codes; empty for accepted events |

Accepted rows have `validation_status = ACCEPTED` and `deduplication_status = UNIQUE`.

## Reconciliation

Every run must prove:

`input_count = accepted_count + rejected_count + duplicate_count`

After output, Spark must read each Parquet dataset back and verify its count against the pre-write outcome count. A run without both reconciliations is not successful.

## Local Layout

```text
data/local/spark_runs/
  gate_count=<count>/
    processing_run_id=<uuid>/
      silver/events/event_date=<yyyy-mm-dd>/
      quarantine/rejected/
      quarantine/duplicates/
```

Generated Silver and quarantine files remain excluded from Git. Compact quality and performance evidence is committed instead.

## Compatibility

Adding an optional field may be backward compatible. Removing or renaming a field, changing its type, changing validation semantics, or changing deduplication semantics requires an engineering decision and migration plan.

