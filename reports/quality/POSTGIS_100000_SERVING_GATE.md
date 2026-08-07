# PostgreSQL/PostGIS 100,000-Row Replay Serving Gate

## Result

**PASSED** on 2026-08-07.

Exactly 100,000 controlled `NASA_REPLAY` event messages derived from 10,000 unique original NASA detections were transformed from trusted Silver into checksum-governed Gold, bulk loaded into PostgreSQL/PostGIS, rerun idempotently, and verified. This gate does not represent 100,000 original NASA observations and contains no synthetic events.

## Identity and Runtime

| Item | Value |
|---|---|
| Truth-verification revision | `ed843f0` |
| Gold run ID | `254a44af-067b-45c1-8e38-257677a54796` |
| Database load run ID | `4aeecb3f-e1fe-48a5-adbe-2b8fc20960bb` |
| Manifest idempotency key | `671116934875597faec9100128f1ee32e92d9c27dcf706605e850fd5056d40ec` |
| Spark image digest | `sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3` |
| PostGIS image digest | `sha256:44126d872ac91993766c341e369c539e8196614321765d36a6f1bab0419a5fa5` |
| PostgreSQL | 16.4 |
| PostGIS | 3.4.3 |
| Psycopg | 3.3.4 |

## Gold Reconciliation

| Metric | Result |
|---|---:|
| Accepted replay Silver input | 100,000 |
| Gold Parquet read-back | 100,000 |
| JSON load-artifact read-back | 100,000 |
| Gold transformation duration | 94.243 seconds |
| Gold throughput | 1,061.09 rows/second |
| Manifest artifacts verified | 7 |
| Parquet data files | 4 |
| Parquet bytes including marker | 30,660,288 |
| Load data files | 1 |
| Load-artifact bytes including marker | 226,751,570 |
| Gold source type | 100,000 `NASA_REPLAY` |
| Gold synthetic rows | 0 |

The job wrote inside Docker's Linux filesystem, reconciled both formats, then copied only the completed relative-path manifest and artifacts to the workspace. Every byte size and SHA-256 passed after copying.

## Database Load

| Metric | First execution | Identical-manifest rerun |
|---|---:|---:|
| Manifest rows | 100,000 | 100,000 |
| Staged rows | 100,000 | Not restaged |
| Inserted rows | 100,000 | 0 |
| Already present | 0 | 100,000 |
| Serving read-back | 100,000 | 100,000 |
| Transactional load duration | 63.627 seconds | Idempotent pre-load no-op |
| First-load throughput | 1,571.65 rows/second | Not applicable |
| Successful load-control rows | 1 | Same load identity reused |

The load used PostgreSQL bulk-copy into temporary staging, content-conflict validation, one serving transaction, aggregate rebuild, and post-load `ANALYZE`.

## Replay and Truth Integrity

| Invariant | Result |
|---|---:|
| Event messages | 100,000 |
| Unique replay event IDs | 100,000 |
| Unique original detection IDs | 10,000 |
| `NASA_ORIGINAL` messages | 0 |
| `NASA_REPLAY` messages | 100,000 |
| `SYNTHETIC_SCALE_TEST` messages | 0 |
| `is_synthetic = true` | 0 |
| Replay messages per detection | exactly 10 |
| Unique replay sequence numbers | 100,000 |
| Replay sequence range | 0-99,999 |
| Replay iteration range | 1-10 |
| Non-null parent event IDs | 100,000 |
| Scheduled replay range | 2026-08-07 00:00:00Z to 00:16:39.990Z |
| Lineage summary rows | 10,000 |
| Daily aggregate message sum | 100,000 |

## Spatial, Security, and Failure Controls

| Check | Result |
|---|---|
| Invalid/null geometry, coordinate mismatch, or wrong SRID | 0 |
| Geometry | Valid `Point`, SRID 4326, longitude X / latitude Y |
| API role can select | Yes |
| API role can insert | No |
| Monitoring role can read load control | Yes |
| Public database connect | Denied |
| Data checksums | Enabled |
| Content-conflict probe | 1 differing payload detected |
| Rows after forced rollback | 100,000 |
| Host exposure | `127.0.0.1:55432` only |

No credential was written to a file, report, manifest, or Git.

## Query Performance

Thirty warm local executions were measured after `ANALYZE`.

| Query | p50 | p95 | p99 | Representative plan execution | Access path |
|---|---:|---:|---:|---:|---|
| Daily aggregate | 1.877 ms | 2.727 ms | 5.132 ms | 0.043 ms | One-row aggregate scan |
| Lineage lookup, 10 rows | 1.980 ms | 3.841 ms | 1,100.932 ms | 0.111 ms | B-tree bitmap scan and in-memory sort |
| Spatial bounding box, 10,320 rows | 5.440 ms | 6.756 ms | 7.136 ms | 7.503 ms | GiST bitmap index and heap scan |
| Source summary | 10.929 ms | 12.479 ms | 12.482 ms | 34.000 ms | Index-only scan and aggregate |

The single 1.1-second lineage outlier is retained. The query's p95 was 3.841 ms and its recorded `EXPLAIN (ANALYZE, BUFFERS)` execution was 0.111 ms, so the outlier is attributed to local runtime/host scheduling noise rather than the selected query plan. This is an inference, not a production SLA claim.

## Storage and Resource Evidence

| Metric | 10,000 gate | 100,000 gate | Growth |
|---|---:|---:|---:|
| Database size | 57,422,307 B | 408,867,299 B | 7.12x |
| Event-detail heap | 11,706,368 B | 136,536,064 B | 11.66x |
| Event-detail indexes | 5,750,784 B | 41,549,824 B | 7.23x |
| Event-detail total relation | 34,136,064 B | 385,294,336 B | 11.29x |
| Physical named volume | not previously recorded | 888.2 MB | includes WAL and engine overhead |

At the final snapshot:

- container CPU: 1.16%;
- memory: 564.6 MiB of 2 GiB;
- processes: 6;
- shared buffers: 512 MiB;
- maximum connections: 30;
- block I/O: 224 MB read / 1.71 GB written.

The indexes comprised approximately 15.07 MB primary key, 18.57 MB lineage/replay, 4.24 MB GiST geometry, 2.77 MB detection/time, 0.85 MB source/time, and 0.02 MB BRIN event time.

The difference between the 385.29 MB total relation and the 178.09 MB combined main heap and indexes is principally TOAST storage for the duplicated full `event_payload` JSONB. The 888.2 MB volume additionally includes WAL and PostgreSQL storage overhead. This is the gate's main optimization finding.

## Scale Comparison

| Metric | 10,000 original gate | 100,000 replay gate |
|---|---:|---:|
| Gold duration | 22.243 s | 94.243 s |
| Gold throughput | 449.58 rows/s | 1,061.09 rows/s |
| Database load duration | 3.839 s | 63.627 s |
| Database load throughput | 2,604.85 rows/s | 1,571.65 rows/s |
| Warm spatial p95 | 2.823 ms | 6.756 ms |
| Warm summary p95 | 6.296 ms | 12.479 ms |

Gold throughput improved as fixed Spark costs were amortized. Database load throughput decreased because the wider replay rows, indexes, full JSONB copy, aggregate work, WAL, and statistics refresh scaled with the larger transaction. Two gates do not prove linear scalability.

## Test Evidence

- 46 automated tests passed in 1.132 seconds.
- The verifier now requires explicit expected original, replay, synthetic, and unique-detection counts.
- A dedicated test proves that 100,000 replay messages reconcile to 10,000 detections without claiming 100,000 originals.

## Environmental Event

Docker Desktop's Linux engine became unavailable after the Gold command returned. After Docker Desktop restarted, the preserved container reported exit code 0, `OOMKilled=false`, and the complete 100,000/100,000/100,000 reconciliation above. The run was not repeated or silently replaced. This event is a laptop-runtime limitation and not a Spark job failure.

## Limitations and Gate Decision

- This is a local single-node serving test, not RDS, Multi-AZ, TLS, failover, backup-restore, or IAM-authentication evidence.
- Replay measurements repeat the spatial and observation-time distribution of 10,000 original detections exactly ten times.
- All records retain one original event date, so date-partition diversity remains unmeasured.
- Warm local latency is not an API or cloud SLA.
- Full JSONB duplication creates material TOAST and WAL overhead.
- Linear projection from the measured relation suggests approximately 3.85 GB at one million rows and 38.5 GB at ten million before operational headroom, but those are estimates, not achieved results.

Phase 6C passes. The one-million gate is blocked pending an approved storage-layout A/B design that preserves auditability while reducing duplicated hot-table payload storage. PostgreSQL, Spark, and Kafka are stopped; the 100,000-row PostgreSQL named volume is preserved.
