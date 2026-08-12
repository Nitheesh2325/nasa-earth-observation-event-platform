# Engineering Decisions

This document explains the choices that shape the platform and the tradeoffs behind them. Detailed field definitions live in [DATA_DICTIONARY.md](DATA_DICTIONARY.md), measured results in [PERFORMANCE_REPORT.md](../PERFORMANCE_REPORT.md), and supporting evidence in [VERIFICATION_RESULTS.md](../reports/VERIFICATION_RESULTS.md).

## Deterministic event identity and lineage

**Problem:** NASA FIRMS rows do not provide a universally guaranteed event UUID, but deduplication, replay, Kafka, Spark, and serving all need stable identities.

**Decision:** Normalize the source dataset, satellite, acquisition time, coordinates, and product version, then hash their deterministic serialization. Keep three identities separate: `event_id` for one message, `detection_id` for the underlying observation, and `lineage_root_id` for the replay family.

**Reason:** Equivalent source rows receive the same identity across every processing path, while replay messages remain distinct and traceable to the NASA detection that produced them.

**Tradeoff:** Changing the normalization algorithm is a breaking contract change. The selected 10,000-record sample is reproducible by event-ID ordering, but it is an engineering benchmark rather than a statistically representative scientific sample.

## Controlled replay rather than inflated source claims

**Problem:** Larger workloads are needed to test Kafka, Spark, recovery, and serving, but replayed messages must not be presented as additional NASA observations.

**Decision:** Generate deterministic `NASA_REPLAY` events from the 10,000 selected detections. Preserve source measurements and observation time, add replay identity and schedule fields, and report event-message count separately from underlying-detection count.

**Reason:** The same input always produces the same ordered byte stream, making throughput and reconciliation tests repeatable without changing data provenance.

**Tradeoff:** Replay tests system behavior, not a larger or more diverse scientific population. The complete local pipeline is verified at one million replay events; the separate 10M experiment verifies generation and read-back only.

## Explicit schemas and mutually exclusive Spark outcomes

**Problem:** Silent coercion or dropped rows would make performance and quality claims unreliable.

**Decision:** Use explicit Spark schemas and DataFrame validation. Every input row is classified exactly once as accepted, rejected, or duplicate. Write outcomes separately and independently read Parquet back before accepting a run.

**Reason:** Input counts, output counts, invalid records, and duplicates can be reconciled without collecting the complete dataset to the driver.

**Tradeoff:** Additional validation and count actions add runtime. That cost is retained because correctness is more important than a faster but unverifiable benchmark.

## Kafka keys, topics, and streaming recovery

**Problem:** Replay events need per-lineage ordering, bounded local resource use, explicit failure routes, and restartable streaming state.

**Decision:** Key replay messages by `lineage_root_id`, retain `event_id` for deduplication, and use six local replay partitions. Keep replay, rejected, and dead-letter topics separate. Structured Streaming uses explicit offset ranges, event-time watermarks, and independent checkpoints for Bronze, accepted, and rejected outputs.

**Reason:** Events for one detection remain ordered within a partition while separate lineages can process concurrently. Independent query state makes recovery behavior observable and prevents incompatible sinks from sharing checkpoints.

**Tradeoff:** The local KRaft deployment is a single broker and does not prove replication or failover. Separate streaming queries read Kafka more than once, which increases local overhead.

## Parquet as the analytical boundary

**Problem:** Streaming output, analytical products, and API serving have different storage and access requirements.

**Decision:** Keep immutable Bronze inputs, partitioned Silver Parquet, and compact Gold Parquet as the durable data layers. Gold includes checksums, counts, aggregates, and partitioned PostgreSQL load files. PostgreSQL is rebuilt from Gold rather than treated as the analytical source of truth.

**Reason:** Parquet supports Spark processing and independent read-back, while the database can be optimized for bounded API queries without duplicating the complete canonical payload.

**Tradeoff:** The platform maintains explicit promotion and reconciliation steps between layers. Raw streaming output can produce small files, so downstream consumers use compact Gold rather than assuming streaming Silver is optimized.

## Compact PostgreSQL/PostGIS serving

**Problem:** The API needs low-latency relational, lineage, and geospatial queries, but storing the full event JSON alongside typed columns created unnecessary storage overhead.

**Decision:** Use a compact typed serving projection with SRID 4326 geometry, B-tree and GiST indexes, daily and lineage aggregates, and transactional staging. Use the Gold manifest checksum as the load idempotency key and reject an existing event ID with different governed content.

**Reason:** Typed columns and PostGIS support bounded spatial and lineage queries while Gold remains the rebuild authority. An identical one-million-row reload inserts zero rows; a content conflict rolls back without changing serving data.

**Tradeoff:** The unpartitioned table is appropriate for the measured one-million-row local boundary. A different layout should follow managed-cloud measurements rather than speculative complexity.

## Read-only FastAPI and bounded caching

**Problem:** Dashboard access must not expose database-owner privileges or allow unbounded queries, and repeated aggregate reads should not repeatedly scan PostgreSQL.

**Decision:** FastAPI uses a non-owner role with forced read-only transactions, parameterized SQL, explicit request and response models, seek pagination, bounded time windows, and bounded spatial boxes. Cache only successful summary and daily aggregate responses with deterministic keys, a 60-second TTL, fixed entry/byte limits, explicit bypass, and PostgreSQL fallback.

**Reason:** The API becomes the only presentation boundary, validation remains consistent, and cache replacement does not change endpoint contracts.

**Tradeoff:** The local cache is process-scoped and provides no distributed invalidation. Health, lineage, spatial detail, invalid requests, and failed responses are intentionally not cached.

## One Airflow DAG with stable run metadata

**Problem:** The batch path needs ordered execution, bounded retries, failure propagation, and safe reruns without adding an orchestration framework around Airflow.

**Decision:** Use one production-style DAG for extraction, canonical transformation, replay, Spark, Gold, PostgreSQL loading, and verification. Derive stable run identities from logical inputs, store atomic stage receipts, enforce explicit timeouts, and reuse successful receipts on the same logical rerun.

**Reason:** Operators can see exactly which stage ran, failed, retried, or was reused. The workflow remains easy to inspect and test.

**Tradeoff:** Airflow is run in Linux because native Windows is unsupported. The local integration profile proves orchestration behavior at a bounded size, not pipeline scale.

## API-only Streamlit dashboard

**Problem:** A recruiter-facing interface is useful, but direct SQL or copied aggregation logic would create a second serving implementation.

**Decision:** Build the dashboard in Streamlit and consume only the six FastAPI GET endpoints for status, summary, daily activity, spatial events, lineage, and readiness.

**Reason:** Every displayed value passes through the same validation, permissions, bounds, and activity-time semantics as other API clients.

**Tradeoff:** Dashboard availability depends on the API, and Streamlit is optimized for a portfolio demonstration rather than a high-concurrency public application.

## Local release boundary and AWS design

**Problem:** The laptop can run the complete platform at one million events, but a 10M Spark attempt exhausted the configured 3 GiB JVM heap. Increasing local memory would compete with Docker Desktop and the Windows host.

**Decision:** Keep one million events as the complete local validation boundary. Record deterministic 10M generation and independent verification separately, with no 10M Spark claim. Prepare a private, budget-controlled AWS design using S3, EMR Serverless, temporary PostgreSQL/PostGIS, CloudWatch, least-privilege IAM, and reproducible teardown.

**Reason:** The repository reports what was measured instead of hiding the resource limit or overstating scale. Cloud execution can reuse the same contracts when explicitly approved.

**Tradeoff:** AWS infrastructure has not been deployed, managed 5M/10M results do not exist, and actual AWS cost remains $0.00.
