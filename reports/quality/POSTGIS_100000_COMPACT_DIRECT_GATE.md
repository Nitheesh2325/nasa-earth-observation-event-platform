# PostgreSQL/PostGIS 100,000-Record Compact Direct-Load Gate

## Result

**Status:** Passed  
**Gate:** Phase 6E  
**Execution date:** 2026-08-07  
**Gold run:** `254a44af-067b-45c1-8e38-257677a54796`  
**Gold-producing code:** `ed843f0`  
**Compact direct-loader code:** `f644e2a`  
**Post-gate idempotency guard:** `bdcc5a0`

An empty PostgreSQL database was rebuilt from the immutable, checksum-validated governed Gold manifest. Migrations `001` and `003` created the serving model and removed only the redundant `event_payload` JSONB column before data was loaded. The loader staged Gold JSON only transiently and inserted 47 explicit serving fields directly into `serving.event_detail`.

This is a replay scale test representing 10,000 underlying original NASA detections. It is not 100,000 original NASA observations.

## Truth and Reconciliation

| Check | Result |
|---|---:|
| Manifest rows | 100,000 |
| Staged rows | 100,000 |
| Inserted rows | 100,000 |
| Serving rows | 100,000 |
| Unique event IDs | 100,000 |
| Unique NASA detections | 10,000 |
| `NASA_ORIGINAL` messages | 0 |
| `NASA_REPLAY` messages | 100,000 |
| Synthetic messages | 0 |
| `is_synthetic = true` | 0 |
| Invalid PostGIS geometries | 0 |
| Daily-summary message total | 100,000 |
| Lineage-summary rows | 10,000 |

The content-conflict predicate detected one deliberately conflicting governed hash inside a forced-rollback transaction. The serving row count remained 100,000 afterward. The identical-manifest rerun inserted zero rows and returned the original successful load-run identity. The strengthened no-op path also reconciles persisted rows for the manifest's Gold run before returning success.

## Runtime and Storage

| Metric | Full JSONB gate (6C) | Compact direct gate (6E) | Change |
|---|---:|---:|---:|
| Direct load duration | 63.627 s | 44.225 s | -30.49% |
| Event-detail total | 385,294,336 B | 178,159,616 B | -53.76% |
| Event-detail heap | 136,536,064 B | 136,536,064 B | 0% |
| Event-detail indexes | 41,549,824 B | 41,549,824 B | 0% |
| Event-detail TOAST/auxiliary | 207,208,448 B | 73,728 B | -99.96% |
| Database size | 408,867,299 B | 201,642,467 B | -50.68% |
| Physical database directory | 888.2 MB | 513,425,863 B | approximately -42% |

The physical-directory comparison includes PostgreSQL engine files and WAL and therefore is not a pure table-size comparison. The 6E size was captured after load, verification, and idempotency checks.

## Warm Local Query Evidence

Thirty executions per query were measured after loader `ANALYZE`.

| Query | p50 | p95 | p99 | Access path |
|---|---:|---:|---:|---|
| Source summary | 38.660 ms | 71.921 ms | 76.205 ms | Sequential scan + hash aggregate |
| Spatial bounding box | 9.934 ms | 13.362 ms | 13.497 ms | GiST bitmap index/heap scan |
| Detection lineage | 1.094 ms | 2.432 ms | 8.653 ms | B-tree bitmap index/heap scan + sort |
| Daily aggregate | 1.332 ms | 2.394 ms | 6.315 ms | One-row sequential scan |

These are warm, local, single-client measurements on a mechanical-drive laptop. They do not establish production concurrency or cloud latency. The broad source-summary query intentionally scans all 100,000 detail rows; API/dashboard summary endpoints should use governed aggregate tables.

## Runtime Boundary

- PostgreSQL 16.4 and PostGIS 3.4.3.
- Digest-pinned image: `postgis/postgis@sha256:44126d872ac91993766c341e369c539e8196614321765d36a6f1bab0419a5fa5`.
- Two-CPU, 2-GiB container limit; `shared_buffers=512MB`; `max_connections=30`.
- Captured idle/post-verification container snapshot: 357.7 MiB memory, 6.10% CPU, 225 MB read / 1.02 GB written block I/O.
- Serving detail retains 47 columns, 15 constraints, and 6 indexes; `event_payload` is absent.
- `eo_api_readonly` can select but cannot insert; `eo_monitoring` can read load controls; `public` cannot connect.

## Limitations

- The gate used one local client and does not test concurrent API/dashboard workload.
- The conflict proof validates detection and rollback; it is not a chaos or crash-recovery test.
- The compact table is rebuildable only while governed Gold artifacts and manifests are retained.
- The physical database volume is disposable test infrastructure and contains no authoritative analytical truth.
- One million rows remain prohibited until a separate execution plan and laptop/cloud resource decision are approved.
