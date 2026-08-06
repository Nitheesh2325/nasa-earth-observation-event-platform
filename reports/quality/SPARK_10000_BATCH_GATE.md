# Spark 10,000-Record Bronze-to-Silver Gate

## Result

**Status:** Passed

**Processing run ID:** `08268edb-13d3-4d75-9e4f-1a06688d6cea`

## Reconciliation

| Outcome | Before write | Parquet read-back |
|---|---:|---:|
| Accepted Silver | 10,000 | 10,000 |
| Rejected quarantine | 0 | 0 |
| Duplicate quarantine | 0 | 0 |
| Total input | 10,000 | 10,000 reconciled outcomes |

The governing equation `10,000 = 10,000 + 0 + 0` passed. The input checksum matched the admitted scale-gate checksum.

## Quality Checks

- Explicit version 1 `StructType` used; schema inference was disabled.
- UTC Spark session timezone configured.
- Required identity, lineage, classification, timestamp, coordinate, schema-version, and measurement rules evaluated with DataFrame expressions.
- Stable `event_id` deduplication used an ordered window.
- Rejected and duplicate outputs were physically separated from Silver.
- Silver was partitioned by `event_date`.
- Parquet output was read back before success was recorded.
- An independent container read verified 10,000 rows and required derived fields.
- A three-record integration fixture reconciled one accepted, one rejected, and one duplicate event.
- Twenty-five automated unit tests passed.

## Output

- Silver Parquet files: 8
- Silver bytes: 3,913,774
- Partition: `event_date=2026-04-01`
- Generated data and manifests: ignored local storage
- Committed evidence contains no NASA key or full dataset

## Gate Decision

The 10,000-record Spark batch gate is complete. Do not begin the 100,000-record gate until its controlled replay or synthetic-generation contract and implementation plan are approved.

