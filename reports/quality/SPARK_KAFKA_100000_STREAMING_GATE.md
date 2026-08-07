# Spark-Kafka 100,000-Message Structured Streaming Gate

## Result

**Passed** on 2026-08-07. Exactly 100,000 controlled `NASA_REPLAY` messages derived from 10,000 unique NASA detections were processed. They are replay messages, not 100,000 new NASA observations.

## Runtime identity

| Item | Value |
|---|---|
| Producer pipeline revision | `14b28ed` |
| Verification revision | `82d1999` |
| Kafka image digest | `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| Spark-Kafka image digest | `sha256:d92fdb4dc4cc1febc451308ea17880f48b511f65528cc792120a2345b9d6fff3` |
| Spark | 4.0.2, `local[4]` |
| Connector | `spark-sql-kafka-0-10_2.13:4.0.2` |
| Spark limit | 4 CPUs, 3 GiB; 2-GiB driver |
| Kafka limit | 2 CPUs, 1.5 GiB |
| Trigger | Available Now |
| Maximum offsets per trigger | 10,000 |
| Shuffle partitions | 16 |
| Watermark | 10 minutes on `scheduled_replay_timestamp` |
| Deduplication key | `event_id` |

The Spark command used the image by digest and contained no `--packages`, `--jars`, Ivy path, or runtime dependency cache.

## Governed source and producer boundary

| Item | Value |
|---|---|
| Replay artifact SHA-256 | `9380341108650b2a5b536f9245148abf572883eb7b13ba0c332d0583fb5e0b0a` |
| Artifact messages | 100,000 |
| Unique NASA detections represented | 10,000 |
| Producer run ID | `aa54fc69-b770-4d57-99e5-08ae5ab17a61` |
| Attempted / acknowledged / offset delta | 100,000 / 100,000 / 100,000 |
| Delivery failures / unflushed | 0 / 0 |
| Producer duration | 11.287 seconds |
| Producer throughput | 8,859.67 records/second |

| Partition | Start | End | Messages |
|---:|---:|---:|---:|
| 0 | 16,482 | 32,942 | 16,460 |
| 1 | 16,112 | 32,212 | 16,100 |
| 2 | 16,941 | 33,861 | 16,920 |
| 3 | 17,189 | 34,359 | 17,170 |
| 4 | 16,263 | 32,513 | 16,250 |
| 5 | 17,119 | 34,219 | 17,100 |

Spark consumed only these explicit ranges. Earlier topic history was excluded.

## First execution

Streaming execution ID: `7cabdcb4-b3b0-4e9e-bfac-1f9220a5d49c`.

| Outcome | Count |
|---|---:|
| Kafka Bronze landed | 100,000 |
| Silver accepted | 100,000 |
| Rejected quarantine | 0 |
| Duplicate | 0 |

- input = accepted + rejected + duplicate
- observed partition ranges exactly matched the producer manifest
- all six partitions were represented
- zero rows were dropped by the watermark
- accepted state peaked at 100,000 rows and 46,824,872 bytes
- application duration was 284.073 seconds
- logical end-to-end rate was 352.02 messages per second

The highest reported lag during processing was 15,453 offsets and final lag was zero.

## Per-query results

| Query | Source rows | Nonempty batches | Summed batch time | Rate |
|---|---:|---:|---:|---:|
| Bronze landing | 100,000 | 10 | 38.890 s | 2,571.36 rows/s |
| Accepted Silver | 100,000 | 10 | 127.894 s | 781.90 rows/s |
| Rejected quarantine | 100,000 | 10 | 80.137 s | 1,247.86 rows/s |

The rejected query still parses and validates every message even though it writes no rejected rows.

## Independent Silver verification and recovery

Recovery execution ID: `baa31e5c-12ed-4085-a254-333927d3e52b`.

| Check | Result |
|---|---:|
| New landing input | 0 |
| New accepted input | 0 |
| New rejected input | 0 |
| Maximum lag | 0 |
| Silver rows | 100,000 |
| Unique event IDs | 100,000 |
| Unique lineage roots | 10,000 |
| Accepted source types | `NASA_REPLAY` only |
| Accepted synthetic rows | 0 |
| Counts or files changed | No |
| Recovery and verification duration | 47.846 seconds |

## Storage and resources

| Output | Parquet files | Bytes |
|---|---:|---:|
| Kafka Bronze | 60 | 45,790,714 |
| Silver | 161 | 48,805,944 |
| Rejected | 10 | 56,440 |

Sampled Spark memory ranged from approximately 892 MiB to a peak of 1.61 GiB under its 3-GiB cap. Sampled Kafka memory ranged from approximately 432 MiB to 453 MiB under its 1.5-GiB cap. Spark CPU reached approximately 388% on Docker's per-core scale during stateful processing, within the four-CPU limit.

The Kafka persistent volume measured approximately 1.364 GB after the gate. Docker reported the derived image as 2.15 GB including shared base layers, with approximately 38.28 MB unique to the derived image. The earlier 795,890,344-byte value is Docker's local image inspection size representation; Docker disk-usage reporting uses a different accounting boundary.

## Limitations

- Three independent queries read the same Kafka range, prioritizing isolated checkpoints and evidence over minimum broker reads.
- Silver produced 161 files for 100,000 rows; a separate measured compaction stage is required.
- Resource values are bounded snapshots, not continuous peak telemetry.
- Rejected-topic publishing and dead-letter behavior remain unimplemented.
- The local single broker does not prove replication, failover, TLS, authentication, or authorization.
- No million-record streaming run or AWS deployment occurred.
