# Phase 6A - PostgreSQL/PostGIS Serving Architecture

## Status

Design complete and Phase 6B implemented. The exactly 10,000-row serving gate passed; see `reports/quality/POSTGIS_10000_SERVING_GATE.md`. PostgreSQL is stopped and its named volume is preserved.

## Objective

Add a governed relational serving layer for low-latency API and dashboard queries without turning PostgreSQL into the system of record or weakening Bronze, Silver, and Gold lineage.

## Architectural Boundary

```text
Silver Parquet
    |
    v
Spark Gold transformations and compaction
    |
    +--> Gold Parquet (authoritative analytical products)
    |
    +--> immutable load manifest and reconciliation metrics
              |
              v
        bounded database loader
              |
              v
 PostgreSQL/PostGIS serving database
       |                 |
       v                 v
    FastAPI          operational SQL
       |
       v
    dashboard
```

Bronze and Silver remain immutable file-backed truth. Gold Parquet is the authoritative analytical output. PostgreSQL is a rebuildable projection optimized for filtering, spatial search, summaries, and API pagination. Kafka does not write directly to PostgreSQL in version 1.

## Selected Runtime

| Component | Design selection | Rationale |
|---|---|---|
| PostgreSQL | 16.4 locally; RDS minor must be selected again at deployment | Mature, supported through November 2028, and aligned with the intended Amazon RDS target |
| PostGIS | 3.4.3 locally; RDS patch must match the selected engine minor | Keeps local and RDS extension behavior aligned |
| Local distribution | `postgis/postgis@sha256:44126d872ac91993766c341e369c539e8196614321765d36a6f1bab0419a5fa5` | Reproducible PostGIS-enabled runtime without a native Windows installation |
| AWS database | Amazon RDS for PostgreSQL 16 with the RDS-supported PostGIS 3.4 patch | Managed backups, patching, monitoring, encryption, and Multi-AZ upgrade path |
| Client/loader | Python 3.12 with `psycopg==3.3.4` | Bounded transactional loading, tests, and manifest generation |

PostgreSQL 18 is current, but selecting it locally would not improve this milestone enough to justify drifting from the mature RDS/PostGIS target. Major and extension upgrades require a compatibility gate and recorded migration plan.

## Database Ownership Model

One database, `eo_intelligence`, contains four schemas:

| Schema | Purpose | Writers | Readers |
|---|---|---|---|
| `serving` | Query-optimized event detail, aggregates, and dimensions | loader only | API and analyst roles |
| `load_control` | Immutable load manifests, run status, reconciliation, and watermarks | loader only | operations role |
| `quality` | Database-side quality results and rejected staging counts | loader only | operations and analyst roles |
| `reference` | Governed source-dataset and geographic reference dimensions | migration/loader roles | API and analyst roles |

The application role cannot create objects, load data, modify control records, or read credentials. The migration role is separate from the runtime loader.

## Logical PostgreSQL Model

### `serving.event_detail`

One row per accepted Silver `event_id`. This is a denormalized serving projection, not a second canonical event contract.

| Column group | Key contents |
|---|---|
| Identity | `event_id` primary key, `detection_id`, `lineage_root_id`, `source_record_id` |
| Classification | `source_type`, `source_dataset`, `is_synthetic` |
| Time | `event_timestamp`, `scheduled_replay_timestamp`, `ingestion_timestamp`, `processing_timestamp`, `event_date`, `event_hour` |
| Spatial | `latitude`, `longitude`, `geometry geometry(Point,4326)`, `geohash`, `country_code`, `admin1_code` |
| Measurements | brightness temperatures, radiative power, scan, track, confidence, day/night, satellite, instrument, product version |
| Replay/synthetic lineage | replay run, iteration, sequence, synthetic generation, parent event |
| Operational lineage | ingestion and Spark run IDs, raw object/file/row/hash, Kafka topic/partition/offset/timestamp |
| Contract lineage | schema version, pipeline version, validation, deduplication, enrichment status |
| Serving control | `gold_run_id`, `loaded_at` |

Integrity constraints preserve canonical classification, coordinate ranges, timestamp requirements, replay/synthetic conditional fields, nonnegative Kafka coordinates, and SRID 4326. Longitude is always X and latitude is always Y. The database recomputes geometry from coordinates during loading rather than trusting arbitrary WKT.

Version 1 remains unpartitioned to preserve a simple globally unique primary key on `event_id`. Ten million rows is a practical indexed PostgreSQL scale, while premature date partitioning would be poorly selective for controlled replay records that retain one original observation date. Partitioning is reconsidered only after measured query plans, maintenance time, and growth beyond the 10-million gate justify it.

### Aggregate tables

| Table | Grain | Primary key | Purpose |
|---|---|---|---|
| `serving.event_hourly_spatial` | activity hour, spatial scheme, spatial cell, source type, dataset | all grain columns | Map density and time-slider queries without scanning detail |
| `serving.event_daily_region` | event date, country, admin-1, source type, dataset | all grain columns | Daily regional trends and headline metrics |
| `serving.detection_lineage_summary` | lineage root | `lineage_root_id` | Original-versus-replay counts and lineage drill-down |
| `serving.dataset_daily_summary` | event date, dataset, source type | all grain columns | Source volume, quality, and synthetic disclosure |
| `serving.platform_summary` | one governed Gold run | `gold_run_id` | Precomputed portfolio/dashboard totals and reconciliation |

H3 is not added as a PostgreSQL extension. Spark will derive a versioned `spatial_cell_scheme` and `spatial_cell_id` before loading; until H3 enrichment is approved, the existing geohash may serve as the bounded spatial-grid key.

### Reference and control tables

| Table | Purpose |
|---|---|
| `reference.source_dataset` | Approved dataset identifiers, provider, instrument, and active contract version |
| `reference.source_type` | The three governed classification values and synthetic truth rule |
| `load_control.gold_run` | Gold transformation identity, input/output locations, checksums, row counts, Git revision, and status |
| `load_control.database_load_run` | Idempotency key, source Gold run, started/completed times, counts, duration, status, and failure summary |
| `load_control.loaded_artifact` | Per-file URI, size, checksum, expected rows, and load status |
| `quality.load_quality_metric` | Named count/rate metric tied to a load run |

## Index Strategy

Indexes are added only for measured access paths:

| Table | Index | Query served |
|---|---|---|
| `event_detail` | unique B-tree on `event_id` | idempotency and event lookup |
| `event_detail` | B-tree on `(detection_id, event_timestamp DESC)` | detection history |
| `event_detail` | B-tree on `(lineage_root_id, scheduled_replay_timestamp)` | lineage drill-down and replay order |
| `event_detail` | GiST on `geometry` | bounding-box, intersection, and proximity filters |
| `event_detail` | B-tree on `(source_type, event_timestamp DESC)` | disclosure-aware recent-event queries |
| `event_detail` | BRIN on `event_timestamp` | low-cost broad time-range scans when physical correlation exists |
| Aggregate tables | B-tree primary keys in dashboard filter order | index-only aggregate lookup |

No index is added independently to every foreign-key-like or low-cardinality field. Index size, write amplification, cache hit ratio, and `EXPLAIN (ANALYZE, BUFFERS)` evidence must justify additions.

## Gold-to-Database Loading Contract

1. Spark writes compacted, versioned Gold Parquet and an immutable manifest.
2. The manifest identifies every artifact by URI, byte size, SHA-256, row count, schema version, Gold run ID, and upstream Silver run IDs.
3. A unique manifest idempotency key rejects a concurrent duplicate load identity before serving rows change.
4. Data enters a load-run-specific staging table with no application privileges.
5. Database constraints and load validation run before serving tables change.
6. One transaction inserts new event IDs and replaces the aggregate snapshot for the same Gold run.
7. The loader records pre-load, staged, inserted, existing-id, rejected, and post-load counts.
8. The transaction commits only when reconciliation passes; otherwise it rolls back and records a safe failure outside the data transaction.
9. Re-running a successful manifest is a no-op proven by its idempotency key and artifact checksums.

Required reconciliation:

```text
manifest_rows = staged_valid_rows + staged_rejected_rows
staged_valid_rows = inserted_rows + already_present_rows
post_load_rows = pre_load_rows + inserted_rows
aggregate_source_rows = aggregate_input_rows
```

`event_id` conflicts are accepted only when the existing row's governed content hash matches. A different payload for the same ID is a hard integrity failure, not an update.

Local Phase 6B used a manifest-governed JSONL export and PostgreSQL bulk-copy semantics. AWS loading will use a private, short-lived ECS/Fargate loader task or equivalent controlled job close to RDS. FastAPI is never the bulk-loader path, and row-at-a-time inserts are prohibited for scale gates.

## API-Facing Query Contract

FastAPI receives read-only access through stable views or reviewed parameterized queries. The first API surface is bounded to:

- summary totals explicitly split by source type and unique detections versus event messages;
- paginated event search by time, source type, dataset, confidence, and bounding box;
- one-event and one-lineage drill-down;
- regional daily trends;
- spatial density cells for a time window.

Detail pagination uses a deterministic seek cursor, not deep `OFFSET`. Every query has a maximum time range, result limit, statement timeout, and explicit sort. Geometry is returned as GeoJSON only at the API boundary. The API never exposes raw object paths, hashes, internal run metadata, database errors, or secrets by default.

## Local Resource Envelope

| Resource | Phase 6B limit |
|---|---:|
| Database container CPU | 2 cores |
| Database container memory | 2 GiB |
| Shared buffers | approximately 512 MiB, measured rather than assumed final |
| Connections | 30 maximum; application pool capped below this |
| Docker storage | named volume on Docker's Linux filesystem |
| Host binding | `127.0.0.1` only |
| First load | exactly 10,000 accepted rows |
| Optional next load | 100,000 only after the 10,000 gate passes |

Kafka and Spark remain stopped during the first database gate. A 10-million-row database is not built on this laptop without a separate measured capacity decision.

## Capacity Estimate

Estimates are planning ranges and must be replaced by measured `pg_total_relation_size` evidence at each gate.

| Scale | Heap estimate | Index estimate | Steady database estimate | Temporary/WAL headroom |
|---:|---:|---:|---:|---:|
| 10,000 | 15-30 MiB | 10-25 MiB | 25-55 MiB | 100 MiB |
| 100,000 | 150-300 MiB | 100-250 MiB | 250-550 MiB | 1 GiB |
| 1,000,000 | 1.5-3 GiB | 1-2.5 GiB | 2.5-5.5 GiB | 5-10 GiB |
| 10,000,000 | 15-30 GiB | 10-25 GiB | 25-55 GiB | 30-60 GiB during initial load/index work |

Gold aggregate tables should remain orders of magnitude smaller than event detail. AWS storage begins with at least 100 GiB of encrypted gp3 and storage autoscaling bounded by a cost alarm; the actual selection follows the 1-million-row measurement.

## AWS Production Topology

- Amazon RDS for PostgreSQL 16 in private subnets, encrypted with a customer-managed KMS key.
- PostGIS version selected from the exact RDS engine minor's supported extension list.
- No public endpoint; security groups allow only the loader, API service, and approved administrative path.
- Secrets Manager provides rotated credentials; IAM authentication is preferred for short-lived application and loader access where driver behavior is proven.
- Multi-AZ is required for a production claim; a Single-AZ instance may be used only for a clearly labeled, time-bounded portfolio cost gate.
- Automated backups, point-in-time recovery, deletion protection, performance insights/Database Insights, enhanced monitoring, and CloudWatch log exports are enabled for production.
- RDS Proxy is added only if measured connection concurrency or serverless API scaling justifies its recurring cost.
- Read replicas are added only after read pressure is measured; Gold Parquet remains the analytical source for large scans.

## Security

- TLS is required in AWS and preferred locally when application integration begins.
- Separate `migration`, `loader`, `api_readonly`, `analyst_readonly`, and `monitoring` roles use least privilege.
- Default privileges are revoked from `PUBLIC`; schema usage and table grants are explicit.
- Credentials live in `.env` locally and Secrets Manager in AWS and never enter Git, manifests, logs, or screenshots.
- Queries are parameterized; identifiers come only from application-owned allowlists.
- Row-level security is unnecessary for the public NASA-derived portfolio dataset and is not enabled without a tenant boundary.
- Database logs exclude full event payloads and credentials; operational IDs are sufficient for tracing.
- Backup restore is tested before a production-ready claim.

## Monitoring and Maintenance

Record for each load and query gate:

- load rows, duration, rows/second, bytes, WAL growth, and reconciliation;
- database, table, and index sizes;
- constraint failures and content-hash conflicts;
- p50, p95, and p99 API query latency from a fixed workload;
- slow query count, locks, deadlocks, connection utilization, cache hit ratio, temporary bytes, and transaction rollback rate;
- autovacuum/analyze freshness, dead tuples, checkpoint pressure, storage headroom, CPU, memory, and freeable memory;
- representative query plans with buffer evidence.

Bulk loads are followed by `ANALYZE`. Autovacuum stays enabled. Manual `VACUUM FULL`, routine reindexing, and arbitrary parameter tuning are prohibited without measured evidence because they create avoidable locks or operational risk.

## Backup and Recovery

Local named volumes are disposable and do not qualify as backups. Schema migrations, deterministic Gold manifests, and Gold Parquet must rebuild the serving database. AWS additionally uses automated backups and point-in-time recovery. The production gate must prove both a manifest-driven rebuild into an empty database and one backup restore rehearsal with recorded recovery time and recovery point.

## Phase 6B Bounded Implementation Gate

Phase 6B completed the following approved work:

1. pin the exact local image tag and digest;
2. keep Kafka stopped, run one bounded Silver-to-Gold Spark job, stop Spark, and add the PostgreSQL service;
3. add reviewed migrations, role/bootstrap definitions, a bounded bulk loader, and tests;
4. load exactly 10,000 preserved accepted Silver events through a versioned Gold projection;
5. verify PostGIS coordinates, constraints, idempotent rerun, counts, sizes, query plans, and resource use;
6. stop PostgreSQL while preserving its named volume;
7. commit only code, contracts, compact fixtures, and evidence—not database files or generated datasets.

## Phase 6B Completion Criteria

- Exact PostgreSQL image digest and actual PostgreSQL/PostGIS versions are recorded.
- Automated schema, constraint, role, loader, and query tests pass.
- Exactly 10,000 rows reconcile from admitted input through Gold, staging, and serving read-back.
- Zero source-classification or synthetic-label violations occur.
- Geometry contains 10,000 valid SRID-4326 points matching longitude/latitude.
- Duplicate manifest execution inserts zero additional rows.
- A conflicting payload for an existing event ID fails safely and leaves serving data unchanged.
- Aggregate totals reconcile to detail and distinguish event messages from unique detections.
- Representative time, spatial, lineage, and summary queries have recorded plans and latency.
- Runtime, throughput, storage, index size, and container resources are recorded.
- Secrets are absent from Git and logs, database access is localhost-only, and roles prove least privilege.
- Limitations are documented and PostgreSQL is stopped after the gate.

## Risks and Decisions Deferred

| Risk | Control |
|---|---|
| Wide detail rows and many indexes inflate storage | Measure at 10,000 and 100,000; retain only query-justified indexes |
| Replay events cluster on original event time | Avoid premature date partitioning; use scheduled activity time in replay-oriented aggregates |
| Spatial queries scan too many points | GiST plus bounded viewport/time filters; serve preaggregated grid cells for maps |
| Loader reruns duplicate or mutate events | Manifest idempotency, primary key, governed content hash, and transaction rollback |
| PostgreSQL becomes an analytics bottleneck | Keep large scans in Gold Parquet; serve only bounded detail and aggregates |
| Local RAM contention | Run PostgreSQL alone at the first gate with strict container limits |
| Local/cloud extension drift | Pin both actual versions and execute an RDS compatibility gate before deployment |
| RDS cost persists when idle | Budget alarm, time-bounded portfolio deployment, snapshot decision, and documented teardown |

## Research Basis

- PostgreSQL 16 is supported by the PostgreSQL project through November 2028.
- PostgreSQL recommends running the current minor release within a selected major version.
- Amazon RDS supports PostGIS and publishes the exact extension versions by engine minor.
- AWS recommends keeping autovacuum enabled and upgrading PostGIS to the version supported by the upgraded engine.
- PostGIS geometry GiST indexing and SRID-aware point storage provide the required spatial access path.

Exact versions and image digests are intentionally deferred to implementation day because minor releases and published image manifests can change.

## Primary References

- [PostgreSQL versioning policy and support dates](https://www.postgresql.org/support/versioning/)
- [PostgreSQL 16 documentation](https://www.postgresql.org/docs/16/)
- [PostGIS 3.4 manual](https://postgis.net/docs/manual-3.4/)
- [Amazon RDS PostgreSQL extension version matrix](https://docs.aws.amazon.com/AmazonRDS/latest/PostgreSQLReleaseNotes/postgresql-extensions.html)
- [Amazon RDS guidance for managing PostGIS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.PostGIS.html)
- [Amazon RDS PostgreSQL feature and autovacuum guidance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.html)
