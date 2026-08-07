# Spark 1,000,000-Message Bronze-to-Silver Batch Gate

## Result

**Status:** Passed  
**Gate:** Phase 6G.2  
**Processing run ID:** `b3b54428-dcac-486e-b9ec-7a495fb27f8e`  
**Pipeline revision:** `bc28ec0`  
**Independent verifier revision:** `4228f10`

The admitted Phase 6G.1 artifact contains one million controlled `NASA_REPLAY` messages derived from 10,000 original NASA detections. It contains zero synthetic events and is not one million original NASA observations.

## Immutable input

| Field | Value |
|---|---|
| Physical replay execution | `3224f997-d2a8-4494-a1a1-e771b4804739` |
| Input rows | 1,000,000 |
| Input bytes | 1,842,603,090 |
| Input SHA-256 | `67d32855fe894c6b4e2a5237045f25db4374762320d6a561dc0d438efa2e7778` |
| Source type | `NASA_REPLAY` |
| Underlying NASA detections | 10,000 |
| Replay factor | 100 |

## Runtime boundary

| Property | Value |
|---|---|
| Spark | 4.0.2, `local[4]` |
| Image | `apache/spark:4.0.2-python3` |
| Image digest | `sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3` |
| Container limit | 4 CPUs, 4 GiB |
| Driver memory | 3 GiB |
| Driver result limit | 512 MiB |
| Shuffle partitions | 32 |
| Default parallelism | 4 |
| Kafka/PostgreSQL | stopped |

## Job reconciliation

| Outcome | Before write | Parquet read-back |
|---|---:|---:|
| Accepted Silver | 1,000,000 | 1,000,000 |
| Rejected quarantine | 0 | 0 |
| Duplicate quarantine | 0 | 0 |
| Total input | 1,000,000 | 1,000,000 reconciled outcomes |

The equation `1,000,000 = 1,000,000 + 0 + 0` passed. Outputs were written with error-if-exists semantics beneath the immutable processing run ID and read back before the success manifest was created.

## Independent Silver verification

The separate verifier did not trust the batch manifest's truth counts.

| Check | Result |
|---|---:|
| Silver rows | 1,000,000 |
| Unique event IDs | 1,000,000 |
| Unique detection IDs | 10,000 |
| `NASA_REPLAY` | 1,000,000 |
| Synthetic true | 0 |
| Replay sequence | 0-999,999 |
| Replay iteration | 1-100 |
| Scheduled boundary | `2026-08-08T00:00:00.000Z` to `2026-08-08T02:46:39.990Z` |
| Null parents | 0 |
| Null event dates | 0 |
| Null processing timestamps | 0 |
| Null Spark run IDs | 0 |
| Verification duration | 27.243 s |
| Verification throughput | 36,706.53 rows/s |

## Performance and storage

| Metric | 10,000 | 100,000 | 1,000,000 |
|---|---:|---:|---:|
| Job duration | 36.029 s | 61.232 s | 149.502 s |
| Throughput | 277.56/s | 1,633.13/s | 6,688.88/s |
| Silver Parquet files | 8 | 16 | 32 |
| Silver Parquet bytes | 3,913,774 | 38,399,832 | 214,918,941 |

The million-row output is smaller than a simple tenfold projection from 100,000 because repeating the same underlying measurement population improves columnar compression. The 32 Parquet data files average approximately 6.72 MB, still below typical cloud analytics target sizes; Gold compaction remains required.

Observed container snapshots:

- early processing: 611.2 MiB / 4 GiB, 388.51% CPU;
- middle processing: 1.694 GiB / 4 GiB, 404.78% CPU;
- high-water observation: 3.745 GiB / 4 GiB, 392.99% CPU, 241 MB read / 370 MB written block I/O;
- independent verifier observation: 928.6 MiB / 4 GiB, 394.09% CPU.

The high-water snapshot used 93.62% of the container limit. The run did not OOM and finished below the two-hour threshold, but this is insufficient headroom for a five-million local Spark claim.

## File evidence

- Silver data: 32 Parquet files, 214,918,941 data bytes.
- Silver directory including checksum/metadata files: 216,598,329 bytes across 66 files.
- Empty rejected output: one schema-bearing Parquet file; read-back count zero.
- Empty duplicate output: one schema-bearing Parquet file; read-back count zero.
- Free D: disk after the gate: 989,099,880,448 bytes.
- Actual cloud cost: USD 0.00.

## Limitations

- Input events share one original NASA observation date, so multi-date partition behavior remains unmeasured.
- The job is bounded batch Spark on one laptop, not a distributed cluster or concurrency benchmark.
- Docker bind mounts and the mechanical D: drive affect runtime and I/O.
- Peak resource evidence is sampled rather than continuous telemetry.
- Zero natural rejection and duplicate outcomes occurred; committed fixtures cover those branches.
- The job uses memory-and-disk persistence; the high-water observation shows that larger local gates require a different execution environment or redesign.

## Gate decision

Phase 6G.2 passes. Its Silver path is admitted for Phase 6G.5 after the Kafka and Structured Streaming subgates. Phase 6G.3 may publish the immutable one-million replay artifact to the bounded Kafka gate. Spark, Kafka, and PostgreSQL are stopped after verification.
