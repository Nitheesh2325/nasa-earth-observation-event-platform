# PostgreSQL/PostGIS 1,000,000-Row Compact Serving Gate

## Result

**Status:** Passed

**Gate:** Phase 6G.6

**Gold run:** `80083cae-e529-462f-bf06-f7576bbc1ecc`

**Database load run:** `2102a7f5-43fe-4686-b0e2-9e564a49d84f`

**Loader and Gold revision:** `a597fa4`

**Strengthened verification revision:** `63c838f`

An empty PostgreSQL 16.4/PostGIS 3.4.3 database was rebuilt from the Phase 6G.5 checksum-admitted Gold 1.1 manifest. Migrations `001` and `003` created the serving model and promoted the measured compact 47-column projection before any event row was loaded.

The database contains one million controlled `NASA_REPLAY` messages representing 10,000 underlying original NASA detections exactly 100 times each. It does not contain one million original NASA observations and contains zero synthetic records.

## Clean rebuild boundary

- Prior compact 100,000-row evidence was committed before the disposable Compose volume was removed.
- Only the stopped Compose-managed `eo_postgres_data` volume and its stopped `eo-postgres` container were removed.
- The database was recreated from the digest-pinned `postgis/postgis` image at `sha256:44126d872ac91993766c341e369c539e8196614321765d36a6f1bab0419a5fa5`.
- PostgreSQL data checksums report `on`.
- Windows rejected the prior localhost port 55432 before the database started. The Compose host binding was parameterized and the gate used session-selected localhost port 55382. PostgreSQL remained on container port 5432 and the credential remained session-only.

## Manifest and load reconciliation

| Check | Result |
|---|---:|
| Manifest rows | 1,000,000 |
| Checksum-admitted load parts | 4 |
| Declared load-part rows | 1,000,000 |
| Staged rows | 1,000,000 |
| Inserted rows | 1,000,000 |
| Already-present rows on first load | 0 |
| Serving rows after load | 1,000,000 |
| Successful database-load runs | 1 |
| Loaded-artifact row sum | 1,000,000 |

The loader validated the manifest and every recorded artifact byte count and SHA-256 before staging. Staging, conflict inspection, compact insertion, aggregate creation, quality metrics, and load-control success were committed through the existing controlled loader path.

## Truth and replay reconciliation

| Check | Result |
|---|---:|
| Serving rows / unique event IDs | 1,000,000 / 1,000,000 |
| Unique NASA detections | 10,000 |
| `NASA_ORIGINAL` messages | 0 |
| `NASA_REPLAY` messages | 1,000,000 |
| Synthetic messages / `is_synthetic=true` | 0 / 0 |
| Replay sequence | complete 0-999,999 |
| Unique replay sequences | 1,000,000 |
| Replay iterations | complete 1-100 |
| Events per detection | exactly 100 |
| Null replay parents | 0 |

## Geometry, aggregates, and security

| Check | Result |
|---|---:|
| Null, invalid, wrong-SRID, or coordinate-mismatched geometry | 0 |
| Daily event-message total | 1,000,000 |
| Daily unique-event total | 1,000,000 |
| Daily replay-message total | 1,000,000 |
| Lineage summary rows | 10,000 |
| Lineage event-message total | 1,000,000 |
| Events per lineage | exactly 100 |
| API role can select / insert | true / false |
| Monitoring role can read load controls | true |
| Public role can connect | false |
| Event-detail columns / constraints / indexes | 47 / 15 / 6 |
| Duplicated `event_payload` columns | 0 |

PostGIS geometry uses SRID 4326 with longitude as X and latitude as Y.

## Idempotency and conflict proof

The identical manifest rerun returned the original load-run identity, inserted zero rows, and reconciled 1,000,000 already-present rows. Its external duration, including complete manifest and artifact checksum validation, was 48.981 seconds.

A deliberately different governed content hash for one existing event ID was detected inside a forced-rollback transaction. The conflict count was one and the serving row count remained exactly 1,000,000 afterward.

## Performance

| Metric | Value |
|---|---:|
| Loader database duration | 408.006 seconds |
| Loader database throughput | 2,450.94 rows/second |
| External load duration including artifact admission | 417.491 seconds |
| External end-to-end load throughput | 2,395.26 rows/second |
| Idempotent external duration | 48.981 seconds |
| Highest sampled PostgreSQL memory | 1.804 GiB / 2 GiB |
| Final sampled block I/O | 6.49 GB read / 13.9 GB written |

Thirty warm local executions per query were measured after loader `ANALYZE`.

| Query | p50 | p95 | p99 | Measured plan |
|---|---:|---:|---:|---|
| Source summary | 101.121 ms | 143.746 ms | 144.390 ms | Parallel index-only scan on `event_detail_source_time_idx` |
| Spatial bounding box | 231.496 ms | 378.860 ms | 894.409 ms | GiST bitmap index/heap scan |
| Detection lineage | 1.319 ms | 2.383 ms | 608.470 ms | Index scan on `event_detail_lineage_replay_idx` |
| Daily aggregate | 0.850 ms | 1.963 ms | 2.344 ms | One-row sequential scan |

The lineage p99 outlier is retained. Its measured `EXPLAIN ANALYZE` execution time was 0.182 ms, so the latency outlier must not be presented as the database plan cost.

## Storage and WAL

| Metric | Bytes |
|---|---:|
| Database | 1,779,675,619 |
| Event-detail total | 1,756,102,656 |
| Event-detail heap | 1,365,336,064 |
| Event-detail indexes | 390,356,992 |
| PostgreSQL physical data directory | 2,896,650,695 |
| Current WAL directory allocation | 1,073,741,824 |

The physical directory includes PostgreSQL engine files and WAL and is not a pure relation-size measurement.

## Verification and service state

- The strengthened verifier fails closed on detail truth, replay sequence and iteration, exact per-detection frequency, geometry, daily aggregates, lineage aggregates, load controls, roles, conflict rollback, plans, latency, and storage.
- All 60 automated tests passed.
- Actual cloud cost is USD 0.00.
- Kafka and Spark remained stopped throughout the database gate.
- PostgreSQL was stopped after final evidence capture; its completed one-million-row named volume is preserved.

## Limitations

- Warm local timings use one client on a mechanical-drive laptop and do not establish production concurrency or cloud latency.
- Memory was sampled rather than continuously profiled. The highest observed value was approximately 90.2% of the 2-GiB limit.
- The 1.804-GiB memory observation prohibits a larger local PostgreSQL serving claim without a new capacity decision.
- The source-summary query scans the complete event-detail population; the governed aggregate is the intended bounded dashboard path.
- The spatial bounding box used by the fixed verifier covers a large portion of the data and therefore reads many heap pages despite using the GiST index.
- Local named volumes are not backups. Gold Parquet and its checksum-governed manifest remain the authoritative recovery source.

## Gate decision

Phase 6G.6 passes. All six Phase 6G subgates are complete, and the governed one-million local scale gate is closed. Phase 7 is the next milestone and was not started.
