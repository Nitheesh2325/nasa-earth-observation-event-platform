# Performance Report

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

## Next Measurement

Spark Structured Streaming throughput, watermark behavior, checkpoint recovery, rejected-topic routing, dead-letter handling, and end-to-end Kafka-to-Silver latency remain unmeasured. The bounded streaming fixture and connector dependency require separate approval.
