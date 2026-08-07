# Performance Report

## 1,000,000-Message Kafka Replay Gate

**Status:** Passed

| Metric | Value |
|---|---:|
| Attempted / acknowledged / offset delta | 1,000,000 / 1,000,000 / 1,000,000 |
| Delivery failures / unflushed | 0 / 0 |
| Producer duration | 64.367 seconds |
| Producer throughput | 15,536.03 messages/second |
| Diagnostic consumed / unique | 1,000,000 / 1,000,000 |
| Diagnostic duration | 26.066 seconds |
| Diagnostic throughput | 38,364.39 messages/second |
| Invalid / duplicate / key mismatch | 0 / 0 / 0 |
| Broker memory snapshots | 494.9-546.4 MiB / 1.5 GiB |

All six partitions were within -3.40% to +3.02% of the mean. Diagnostic success now fails closed on offset, identity, detection frequency, replay sequence/iteration, source type, synthetic flag, JSON, duplicate, and key/lineage checks. This remains a sequential single-broker local measurement rather than a replicated production throughput claim.

## 1,000,000-Message Spark Batch Gate

**Status:** Passed

Spark 4.0.2 processed the admitted million-message replay artifact with four CPUs, 4 GiB container memory, 3 GiB driver memory, and 32 shuffle partitions.

| Metric | Value |
|---|---:|
| Input / accepted | 1,000,000 / 1,000,000 |
| Rejected / duplicate | 0 / 0 |
| Duration | 149.502 seconds |
| Throughput | 6,688.88 rows/second |
| Silver Parquet | 32 files / 214,918,941 bytes |
| Independent verification | 27.243 seconds / 36,706.53 rows/second |
| Peak observed memory | 3.745 GiB / 4 GiB |

The run reconciled all output read-backs and independent truth checks. Throughput improved from 1,633.13 rows/second at 100,000 records because fixed work was further amortized and replay data compressed efficiently. The 93.62% memory high-water observation prohibits extrapolating this local configuration to five million records.

## 1,000,000-Message Replay Generation Gate

**Status:** Passed

| Metric | Execution 1 | Execution 2 |
|---|---:|---:|
| Controlled replay messages | 1,000,000 | 1,000,000 |
| Underlying NASA detections | 10,000 | 10,000 |
| Duration | 55.911 seconds | 49.777 seconds |
| Throughput | 17,885.72 rows/second | 20,089.41 rows/second |
| Output bytes | 1,842,603,090 | 1,842,603,090 |
| Output SHA-256 | `67d32855...e7778` | identical |

Independent full-artifact verification completed in 53.154 seconds at 18,813.13 rows/second. It recomputed all identities and schedule positions, verified exactly 100 events for each of 10,000 detections, preserved original fields, replay-only classification, zero synthetic flags, bytes, and checksum. This is local JSON generation and validation throughput, not Spark, Kafka, source, or end-to-end throughput.

## PostgreSQL/PostGIS 100,000-row storage A/B gate

- Compact B retained 47 materialized columns and removed only duplicated `event_payload` JSONB.
- All 100,000 rows were identical after excluding only that field; missing rows and identity/hash/lineage mismatches were zero.
- Total relation size fell from 385,294,336 to 178,126,848 bytes, saving 207,167,488 bytes or 53.77%.
- TOAST fell from 207,142,912 bytes to 8,192 bytes, approximately a 99.996% reduction.
- After equal `VACUUM (ANALYZE)`, compact p95 versus full p95 was 18.700 versus 17.247 ms for summary, 9.618 versus 10.851 ms for spatial, and 3.366 versus 4.332 ms for lineage.
- Compact B met the maximum 20% p95 regression rule and is selected for direct-loader implementation.
- The 13.503-second `INSERT ... SELECT` materialization is not a direct Gold bulk-load benchmark.
- Full evidence: `reports/quality/POSTGIS_100000_STORAGE_AB_GATE.md`.

## PostgreSQL/PostGIS 100,000-row replay serving gate

- Gold transformation: 100,000 replay Silver rows to reconciled Gold Parquet and load artifact in 94.243 seconds (1,061.09 rows/second).
- Database bulk load: 100,000 staged and inserted rows in 63.627 seconds (1,571.65 rows/second).
- Identical-manifest rerun: zero inserts and 100,000 already-present rows.
- Truth: 100,000 unique replay event messages, 10,000 unique underlying NASA detections, zero original-message claims, and zero synthetic rows.
- Database size: 408,867,299 bytes; event-detail total: 385,294,336 bytes; physical named volume: 888.2 MB including WAL and engine overhead.
- Warm local p95: daily aggregate 2.727 ms, lineage 3.841 ms, spatial bounding box 6.756 ms, and source summary 12.479 ms.
- One lineage p99 sample reached 1,100.932 ms despite a 0.111-ms measured plan execution; the outlier is retained as local host/runtime noise.
- Full JSONB duplication caused material TOAST and WAL amplification and must be evaluated before the one-million gate.
- Full evidence: `reports/quality/POSTGIS_100000_SERVING_GATE.md`.

## PostgreSQL/PostGIS 10,000-row serving gate

- Gold transformation: 10,000 Silver rows to reconciled Gold Parquet and load artifact in 22.243 seconds.
- Database bulk load: 10,000 staged and inserted rows in 3.839 seconds.
- Identical-manifest rerun: zero inserts and 10,000 already-present rows.
- Database size: 57,422,307 bytes; event-detail relation: 34,136,064 bytes, including 5,750,784 index bytes.
- Warm local query p95: daily aggregate 2.548 ms, lineage 2.823 ms, spatial bounding box 2.823 ms, and source summary 6.296 ms.
- Observed idle post-verification memory: 93.28 MiB within a 2-GiB container limit.
- Full evidence and limitations: `reports/quality/POSTGIS_10000_SERVING_GATE.md`.

## Measurement Policy

Each governed scale gate records the exact input checksum, pipeline revision, runtime, throughput, Spark configuration, output counts, output size, read-back counts, and known hardware limitations. Results from different environments are not treated as directly comparable unless their configurations are equivalent.

## 10,000-Record Bronze-to-Silver Gate

**Status:** Passed

**Run date:** 2026-08-06

**Processing run ID:** `08268edb-13d3-4d75-9e4f-1a06688d6cea`

### Environment

| Property | Value |
|---|---|
| Host | Dell G7 7588 |
| Host RAM | 16 GB |
| Workspace storage | Mechanical D: drive |
| Docker memory available | Approximately 7.65 GiB |
| Container image | `apache/spark:4.0.2-python3` |
| Image digest | `sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3` |
| Container limit | 4 CPUs, 3 GiB RAM |
| Spark | 4.0.2 |
| Spark master | `local[4]` |
| Driver memory | 2 GiB |
| Driver result limit | 256 MiB |
| Shuffle partitions | 8 |
| Default parallelism | 4 |
| Pipeline revision | `9939af1` |

### Input

| Metric | Value |
|---|---:|
| Records | 10,000 |
| JSONL bytes | 15,227,942 |
| SHA-256 | `3f711f63138ae1e5c926d5dcd6edf3a15094ae8f7d8631db4e566b936862ede3` |
| Source type | `NASA_ORIGINAL` |
| Synthetic records | 0 |

### Results

| Metric | Value |
|---|---:|
| Accepted | 10,000 |
| Rejected | 0 |
| Duplicates | 0 |
| Silver read-back | 10,000 |
| Duration | 36.029 seconds |
| Throughput | 277.56 records/second |
| Silver Parquet files | 8 |
| Silver Parquet bytes | 3,913,774 |
| Event-date partitions | 1 |

All records belong to the `event_date=2026-04-01` partition. An independent read verified 10,000 rows, 47 schema fields, and the presence of the six required derived fields.

### Timing Boundary

The job timer begins inside the container immediately before Spark session creation and ends after accepted, rejected, and duplicate Parquet outputs have each been read back and reconciled. It includes Spark startup and all job actions but excludes Docker image pull time and container creation time.

### Interpretation

This result proves correctness at the first scale gate. It is not a cloud benchmark and should not be extrapolated linearly to 10 million records. The mechanical disk, Docker bind mount, Spark startup cost, repeated quality actions, and small input size materially affect throughput.

### Known Limitations

- The input contains one event date, so multi-partition write behavior is not yet measured.
- Zero rejected and duplicate records occurred in the real gate; a separate three-record integration fixture verified those branches.
- Spark emitted a large-plan display warning because of the explicit validation expression set; execution succeeded.
- Docker and the Windows-to-Linux bind mount add overhead not representative of EMR Serverless.
- No shuffle, partition, or cache tuning conclusion is justified from a single small run.

## 100,000-Message Replay Generation Gate

**Status:** Passed

Two independent standard-library generations produced byte-identical 184,078,310-byte JSONL artifacts with SHA-256 `9380341108650b2a5b536f9245148abf572883eb7b13ba0c332d0583fb5e0b0a`.

| Metric | Execution 1 | Execution 2 |
|---|---:|---:|
| Replay messages | 100,000 | 100,000 |
| Unique NASA detections represented | 10,000 | 10,000 |
| Duration | 5.064 seconds | 5.025 seconds |
| Throughput | 19,745.49 records/second | 19,900.14 records/second |
| Output bytes | 184,078,310 | 184,078,310 |

This measures local JSON replay generation and hashing only. It is not Spark processing throughput, Kafka producer throughput, or NASA source throughput. The generator loads and sorts 10,000 admitted originals, then streams 100,000 output messages to the mechanical D: drive.

The measured size fell within the planned 170-190 MB range. Physical run identifiers and wall-clock manifest fields differ; governed event bytes remain identical.

## 100,000-Message Bronze-to-Silver Gate

**Status:** Passed

| Metric | Value |
|---|---:|
| Input replay messages | 100,000 |
| Unique original NASA detections represented | 10,000 |
| Accepted | 100,000 |
| Rejected | 0 |
| Duplicates | 0 |
| Silver read-back | 100,000 |
| Duration | 61.232 seconds |
| Throughput | 1,633.13 records/second |
| Silver Parquet files | 16 |
| Silver Parquet bytes | 38,399,832 |

The bounded environment remained Spark 4.0.2 on `local[4]` with four container CPUs, 3 GiB container memory, 2 GiB driver memory, and 16 shuffle partitions. The timer uses the same job-internal boundary as the 10,000 gate.

Observed throughput increased from 277.56 to 1,633.13 records per second while volume increased tenfold. Duration increased from 36.029 to 61.232 seconds. Fixed Spark startup, validation-plan, write-setup, and reconciliation costs are more heavily amortized at 100,000 records. These two points are insufficient for a scalability curve and must not be extrapolated directly to one million or ten million records.

Independent Silver verification confirmed 100,000 replay IDs, 10,000 detection IDs, complete replay sequence and iteration ranges, correct non-synthetic replay classification, schedule boundaries, and the required derived fields.

## 100,000-Message Kafka Replay Gate

**Status:** Passed

The local single-node Kafka 4.3.1 KRaft broker used six replay partitions, a two-CPU and 1.5-GiB container limit, and the pinned `confluent-kafka==2.15.0` client. The producer used `lineage_root_id` keys, idempotence, all acknowledgements, Zstandard compression, bounded retries, and delivery callbacks.

| Metric | Value |
|---|---:|
| Admitted replay messages | 100,000 |
| Unique original NASA detections represented | 10,000 |
| Acknowledged | 100,000 |
| Delivery failures / unflushed | 0 / 0 |
| Broker offset delta | 100,000 |
| Producer duration | 11.825 seconds |
| Producer throughput | 8,457.01 records/second |
| Serialized value bytes excluding delimiters | 183,978,310 |
| Diagnostic messages consumed | 100,000 |
| Unique event IDs | 100,000 |
| Consumer validation duration | 2.973 seconds |
| Consumer validation throughput | 33,631.75 records/second |
| Invalid JSON / missing IDs / duplicates | 0 / 0 / 0 |
| Key/lineage mismatches | 0 |

Per-partition delivery was `[16,460, 16,100, 16,920, 17,170, 16,250, 17,100]`. All partitions were used; the minimum was 3.4% below and the maximum 3.0% above the ideal mean of 16,666.67. This supports the selected stable key's load distribution for this dataset, but does not establish behavior for different lineage distributions.

The broker used approximately 523.7 MiB of its 1.5-GiB limit during post-run capture. The observed CPU snapshot was 183% of Docker's single-core scale while the container was capped at two CPUs. Producer and diagnostic consumer timers are application-level boundaries; Docker startup, broker warm-up, and evidence commands are excluded. This single-node local result does not measure replication, failover, authentication, sustained lag, or concurrent Spark consumption.

## 100,000-Message Structured Streaming Gate

**Status:** Passed

The admitted replay artifact was republished into a fresh Kafka boundary and processed with the digest-pinned Spark-Kafka image. No dependency was downloaded at runtime.

| Metric | Value |
|---|---:|
| Producer duration | 11.287 seconds |
| Producer throughput | 8,859.67 records/second |
| Structured Streaming application duration | 284.073 seconds |
| Logical end-to-end streaming throughput | 352.02 messages/second |
| Bronze landed | 100,000 |
| Silver accepted | 100,000 |
| Rejected / duplicate | 0 / 0 |
| Unique event IDs / lineage roots | 100,000 / 10,000 |
| Watermark-dropped rows | 0 |
| Peak state-store rows | 100,000 |
| Peak state-store memory | 46,824,872 bytes |

| Query | Source rows | Nonempty microbatches | Summed batch duration | Query rate |
|---|---:|---:|---:|---:|
| Bronze landing | 100,000 | 10 | 38.890 seconds | 2,571.36 rows/second |
| Accepted validation/deduplication | 100,000 | 10 | 127.894 seconds | 781.90 rows/second |
| Rejected quarantine | 100,000 | 10 | 80.137 seconds | 1,247.86 rows/second |

The application-level duration includes Spark startup, three sequential Kafka reads, streaming writes, stateful deduplication, Parquet read-back, offset aggregation, and final reconciliation. It excludes the producer and Docker startup. The three-query design deliberately reads the same source range three times to give each sink an independent checkpoint; therefore logical message throughput is the appropriate end-to-end measure, while the per-query rates describe internal work.

The highest progress-reported lag was 15,453 offsets while processing and returned to zero. Peak observed Spark memory was approximately 1.61 GiB of 3 GiB; Kafka peaked near 453 MiB of 1.5 GiB in sampled observations. The stateful accepted query briefly used close to all four assigned CPUs without breaching memory limits.

The output footprint was 60 Bronze Parquet files totaling 45,790,714 bytes, 161 Silver files totaling 48,805,944 bytes, and 10 rejected files totaling 56,440 bytes. The 161 Silver files for 100,000 rows demonstrate a small-file issue caused by 16 shuffle partitions across ten microbatches. A later Gold/compaction stage should reduce file count based on measured target sizes; the successful gate output is preserved unchanged.

Same-checkpoint recovery processed zero new rows, reported zero lag, and preserved all counts and file sizes. Recovery plus independent Silver verification completed in 47.846 seconds.

## Next Measurement

Rejected-topic publishing, dead-letter behavior, controlled failure recovery, and streaming compaction remain unmeasured. Million-record advancement is not authorized.

## 100,000-Record Compact PostgreSQL Direct-Load Gate

**Status:** Passed

The compact production projection rebuilt an empty database directly from the governed 100,000-row Gold manifest. It represents 100,000 replay messages derived from 10,000 original NASA detections, with zero synthetic records.

| Metric | Value |
|---|---:|
| Staged / inserted / serving rows | 100,000 / 100,000 / 100,000 |
| Direct load duration | 44.225 seconds |
| Full-JSONB direct load baseline | 63.627 seconds |
| Load-duration change | -30.49% |
| Event-detail relation | 178,159,616 bytes |
| Database | 201,642,467 bytes |
| Physical database directory | 513,425,863 bytes |
| Container memory snapshot | 357.7 MiB / 2 GiB |

Warm local p95 latency was 71.921 ms for a full-detail source summary, 13.362 ms for the spatial bounding box, 2.432 ms for lineage lookup, and 2.394 ms for the daily aggregate. The full-detail summary is not the intended dashboard path; governed aggregates should serve that workload. See `reports/quality/POSTGIS_100000_COMPACT_DIRECT_GATE.md` for truth, rollback, plans, security, and limitations.
