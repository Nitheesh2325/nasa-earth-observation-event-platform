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

## Next Measurement

The 100,000-record gate requires a separately approved replay or synthetic generation contract, truthfully labeled records, tests, count reconciliation, and the same performance evidence. Configuration changes must be justified by comparative measurements rather than intuition.

