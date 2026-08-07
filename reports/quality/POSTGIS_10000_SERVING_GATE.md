# PostgreSQL/PostGIS 10,000-Row Serving Gate

## Result

**PASSED** on 2026-08-07.

Exactly 10,000 accepted original NASA-derived Silver events were transformed into checksum-governed Gold artifacts, bulk loaded into PostgreSQL/PostGIS, reconciled, independently queried, rerun idempotently, and verified against spatial, truth-label, security, and conflict-safety controls.

## Revisions and Runtime

| Item | Value |
|---|---|
| Gold build revision | `6fc00c1` |
| Final loader/runtime revision | `57ab83f` |
| PostgreSQL image | `postgis/postgis@sha256:44126d872ac91993766c341e369c539e8196614321765d36a6f1bab0419a5fa5` |
| PostgreSQL runtime | 16.4 |
| PostGIS runtime | 3.4.3 |
| Psycopg | 3.3.4 |
| Spark Gold runtime | `apache/spark@sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3` |
| Gold run ID | `2059a516-019d-40d9-ae44-e76ac29d728a` |
| Database load run ID | `abc5c618-0f31-498d-9cd1-7f1cd3c0f935` |
| Gold manifest idempotency key | `a354fd100f4bc8ea960d053d8a4aebfe034ceb64a455d991bb1f13f03acad6fe` |

## Gold Reconciliation

| Metric | Result |
|---|---:|
| Accepted Silver input | 10,000 |
| Unique Silver event IDs | 10,000 |
| Gold Parquet read-back | 10,000 |
| JSON load-artifact read-back | 10,000 |
| Original NASA-derived events | 10,000 |
| Replay events | 0 |
| Synthetic events | 0 |
| Verified manifest artifacts | 5 |
| Gold transformation duration | 22.243 seconds |
| Gold Parquet files | 2 data files plus success marker |
| Gold Parquet bytes | 4,431,565 |
| Load artifact files | 1 data file plus success marker |
| Load artifact bytes | 19,495,268 |

The Gold transformation ran inside Docker's Linux filesystem and copied only the completed immutable run to the mechanical Windows workspace. Artifact paths are relative to the manifest, and every copied byte size and SHA-256 was verified before loading.

## Database Load Reconciliation

| Metric | First execution | Identical-manifest rerun |
|---|---:|---:|
| Manifest rows | 10,000 | 10,000 |
| Staged rows | 10,000 | Not restaged |
| Inserted rows | 10,000 | 0 |
| Already-present rows | 0 | 10,000 |
| Serving read-back | 10,000 | 10,000 |
| Load duration | 3.839 seconds | Idempotent pre-load no-op |
| Successful load-control rows | 1 | Same load identity reused |

The first load used PostgreSQL bulk-copy protocol into isolated temporary staging, validated conflicts, inserted serving detail, refreshed aggregates, ran `ANALYZE`, and committed one reconciled transaction.

## Truth and Spatial Integrity

| Check | Result |
|---|---:|
| Total event messages | 10,000 |
| Unique event IDs | 10,000 |
| Unique underlying detections | 10,000 |
| `NASA_ORIGINAL` | 10,000 |
| `NASA_REPLAY` | 0 |
| `SYNTHETIC_SCALE_TEST` | 0 |
| `is_synthetic = true` | 0 |
| Invalid/null geometry or SRID mismatch | 0 |
| Daily aggregate message sum | 10,000 |
| Lineage summary rows | 10,000 |

All geometries are valid `Point` values in SRID 4326, with longitude as X and latitude as Y.

## Security and Failure Controls

| Check | Result |
|---|---|
| Host exposure | `127.0.0.1:55432` only |
| Data checksums | Enabled |
| Public database connect | Denied |
| API role can select serving data | Yes |
| API role can insert serving data | No |
| Monitoring role can read load control | Yes |
| Credential persisted to repository or local file | No |
| Differing content hash detected for existing event ID | 1 controlled probe |
| Serving rows after rolled-back conflict probe | 10,000 |

The conflict probe used a transaction forced to roll back. It proved the loader's differing-content guard without changing serving state.

## Query Evidence

Thirty local executions were measured per query after post-load statistics refresh.

| Query | p50 | p95 | p99 | Representative plan |
|---|---:|---:|---:|---|
| Daily aggregate | 1.294 ms | 2.548 ms | 4.663 ms | One-row sequential scan, appropriate for the tiny aggregate |
| Lineage lookup | 1.052 ms | 2.823 ms | 6.060 ms | B-tree index scan on lineage/replay index |
| Spatial bounding box | 2.120 ms | 2.823 ms | 3.308 ms | GiST bitmap index scan plus heap scan |
| Source summary | 4.521 ms | 6.296 ms | 6.965 ms | In-memory hash aggregate over 10,000 detail rows |

These are warm local development measurements, not AWS production latency claims.

## Storage and Resources

| Metric | Result |
|---|---:|
| Database size | 57,422,307 bytes |
| Event-detail heap | 11,706,368 bytes |
| Event-detail indexes | 5,750,784 bytes |
| Event-detail total relation | 34,136,064 bytes |
| PostgreSQL shared buffers | 512 MiB |
| Maximum connections | 30 |
| Container limit | 2 CPUs / 2 GiB |
| Observed stopped-gate snapshot CPU | 0.05% |
| Observed memory | 93.28 MiB / 2 GiB |
| Observed process count | 6 |

The measured 10,000-row total relation is inside the Phase 6A estimate. Storage projections for larger gates remain estimates until measured.

## Test Evidence

- 44 automated unit tests passed in 0.502 seconds before the final runtime.
- Manifest admission tests cover exact gate count, checksum mutation, and portable load-artifact discovery.
- The runtime separately verifies counts, geometry, aggregates, roles, content conflicts, relation sizes, query plans, and latency.

## Corrected Attempts

Two pre-gate environment issues were corrected and are not presented as successful evidence:

1. Direct Parquet commit to the Windows mechanical-drive bind mount stalled and left only zero-byte Spark temporary files. The exact failed run directory was removed. The corrected design writes to Docker's Linux filesystem and copies only a completed manifest-governed run.
2. Windows blocked host port 5432. The localhost-only host binding moved to 55432 while the container and AWS-facing PostgreSQL port remains 5432.

An earlier otherwise successful database run also exposed default `128MB` shared buffers, `100` connections, and missing post-load statistics refresh. It was deliberately discarded; the final clean volume uses the approved `512MB`, 30-connection envelope and executes `ANALYZE` transactionally.

## Limitations

- This gate proves one local PostgreSQL/PostGIS node, not high availability, failover, RDS behavior, backup restore, TLS, or IAM authentication.
- All records are original NASA-derived detections from the governed deterministic 10,000-record sample; this is not a 10-million-record claim.
- The sample occupies one event date, so daily aggregate cardinality is intentionally small.
- The named database volume is preserved locally but is not a backup.
- The full 10-million-row serving database remains an AWS-scale decision after intermediate measurements.

## Gate Decision

Phase 6B is complete. PostgreSQL is stopped, the named volume is preserved, Kafka and Spark are stopped, and no generated dataset or database file is committed to Git.
