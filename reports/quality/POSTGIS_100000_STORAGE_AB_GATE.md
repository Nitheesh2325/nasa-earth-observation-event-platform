# PostgreSQL/PostGIS 100,000-Row Storage A/B Gate

## Result

**PASSED. Compact layout B selected for production-loader implementation.**

The bounded comparison used the preserved 100,000-row `NASA_REPLAY` serving database. Layout A retained the full duplicated `event_payload` JSONB. Layout B retained all 47 materialized serving columns, PostGIS geometry, hashes, Gold identity, source lineage, constraints, indexes, and read grants while removing only `event_payload`.

This selection does not yet replace the production serving table or authorize the one-million-row gate.

## Revisions

| Item | Revision |
|---|---|
| A/B implementation | `ab5d7a9` |
| Atomic local evidence output | `5ee20d3` |
| Encoded maintenance fairness | `940c75b` |

No new dependency or image was added.

## Method and Fairness

1. Preserved `serving.event_detail` as control A.
2. Created `serving.event_detail_compact` using the same types, defaults, checks, foreign keys, and indexes.
3. Removed only `event_payload` from B.
4. Materialized B transactionally from A and ran `ANALYZE`.
5. Repeated the materialization with primary-key conflict handling.
6. Compared every row as JSON after subtracting only A's `event_payload`.
7. Verified truth, geometry, hash, Gold-run, raw-payload-hash, constraint, index, and grant parity.
8. Ran `VACUUM (ANALYZE)` on both tables before the final identical read workload so index-only visibility was comparable.

The 13.503-second B materialization is an `INSERT ... SELECT` projection benchmark, not a claim that B can parse and bulk-load the original 226.75-MB JSONL artifact in that time. Direct compact-loader performance requires a separate gate.

## Row and Governance Parity

| Check | Result |
|---|---:|
| Control A rows | 100,000 |
| Compact B rows | 100,000 |
| First B materialization inserted | 100,000 |
| First B materialization duration | 13.503 seconds |
| Identical second materialization inserted | 0 |
| Second materialization duration including `ANALYZE` | 3.849 seconds |
| Missing event IDs across layouts | 0 |
| Rows unequal after excluding only `event_payload` | 0 |
| Hash, Gold-run, or raw-payload-hash mismatches | 0 |
| Columns retained | 47 |
| Columns removed | only `event_payload` |
| Constraints per table | 15 / 15 |
| Indexes per table | 6 / 6 |
| API role can select B | Yes |
| API role can insert B | No |
| Differing-hash conflict detected | 1 controlled probe |

Complete canonical payload remains authoritative in checksum-governed Gold Parquet and its load artifact. PostgreSQL B remains rebuildable and traceable through event identity, governed content hash, Gold run ID, raw object/file/row/hash lineage, and processing metadata.

## Truth and Spatial Parity

| Invariant | Compact B |
|---|---:|
| Event messages | 100,000 |
| Unique replay event IDs | 100,000 |
| Unique underlying NASA detections | 10,000 |
| Replay messages | 100,000 |
| Synthetic messages | 0 |
| Invalid/null/mismatched geometry | 0 |

This remains a replay scale gate, not 100,000 original NASA observations.

## Storage Result

| Component | A: full payload | B: compact | Difference |
|---|---:|---:|---:|
| Main heap | 136,536,064 B | 136,536,064 B | 0 B |
| Indexes | 41,549,824 B | 41,517,056 B | -32,768 B |
| TOAST total | 207,142,912 B | 8,192 B | -207,134,720 B |
| Total relation | 385,294,336 B | 178,126,848 B after fair maintenance | -207,167,488 B |

Compact B reduces total relation storage by **53.77%** and TOAST storage by approximately **99.996%**. The unchanged main heap demonstrates that the eliminated overhead was out-of-line duplicated JSONB rather than required materialized serving columns.

The physical volume contained both A and B during the experiment and reached 1.218 GB including WAL and PostgreSQL overhead. That volume size is not the expected size of a compact-only production database.

## Fair Query Comparison

Both tables were vacuumed and analyzed immediately before these 30-execution warm local measurements.

| Query | A p95 | B p95 | B change | A plan | B plan |
|---|---:|---:|---:|---|---|
| Source summary | 17.247 ms | 18.700 ms | +8.43% | Aggregate + index-only scan | Aggregate + index-only scan |
| Spatial bounding box | 10.851 ms | 9.618 ms | -11.36% | Aggregate + GiST bitmap/heap scan | Same |
| Lineage lookup | 4.332 ms | 3.366 ms | -22.30% | Sort + B-tree bitmap/heap scan | Same |

All B p95 results meet the selection rule: no unexplained regression greater than 20%. Summary is slightly slower but uses the identical plan and remains below 20 ms locally. Spatial and lineage queries improve.

An initial pre-vacuum comparison showed a misleading summary regression because B could not yet use an index-only scan. That result was rejected for layout selection; the maintenance state and plan difference were diagnosed, both tables were vacuumed/analyzed, and the fair result above was retained.

## Resource Envelope

At the final structural measurement with both A and B present:

- PostgreSQL container memory: 899.6 MiB of 2 GiB;
- CPU snapshot: 8.56%;
- processes: 6;
- block I/O: 837 MB read / 2.99 GB written;
- PostgreSQL remained healthy;
- no stop condition was triggered.

## Decision

Select compact layout B for the next production-loader implementation because it:

- preserves all materialized API, lineage, truth, spatial, and conflict fields;
- preserves constraint, index, and least-privilege behavior;
- keeps complete payload authority in Gold;
- reduces relation storage by 53.77%;
- eliminates nearly all JSONB TOAST duplication;
- meets the query-regression threshold.

## Limitations and Next Gate

- B was materialized from A, not loaded directly from the Gold artifact.
- The current production loader and canonical `serving.event_detail` still implement layout A.
- Full rebuild, direct compact bulk loading, manifest idempotency, and failure rollback for B are not yet proven.
- The A/B database is local single-node evidence, not RDS performance or high availability.
- The expensive full-row parity scan is appropriate for a bounded migration gate but should not become a routine production query.

Phase 6D is complete. Phase 6E must implement a clean compact-only schema and direct Gold-to-compact loader, then repeat the 100,000-row gate before any one-million-row execution. PostgreSQL, Spark, and Kafka are stopped; the A/B volume is preserved.
