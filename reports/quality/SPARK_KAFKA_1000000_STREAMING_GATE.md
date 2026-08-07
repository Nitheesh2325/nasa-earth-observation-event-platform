# Spark-Kafka 1,000,000-Message Structured Streaming Gate

## Result

**Status:** Passed  
**Gate:** Phase 6G.4  
**Streaming run:** `phase6g4-1000000-v1`  
**First execution:** `e39107cc-8985-4281-972d-4662cf6ec157`  
**Recovery execution:** `80aadd30-45a8-48bd-a1ee-c49a6ee360fc`  
**Pipeline revision:** `70eec1f`  
**Profile-aware verifier revision:** `8cffcd6`

Exactly one million controlled `NASA_REPLAY` messages representing 10,000 underlying original NASA detections were processed from the Phase 6G.3 producer's immutable Kafka ranges. They are replay messages, not one million original NASA observations, and contain zero synthetic records.

## Runtime identity and source boundary

| Item | Value |
|---|---|
| Kafka producer run | `41ab9e88-27ab-4936-9e9c-fa7ea181dfcd` |
| Producer offset delta | 1,000,000 |
| Kafka image digest | `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| Spark-Kafka image digest | `sha256:d92fdb4dc4cc1febc451308ea17880f48b511f65528cc792120a2345b9d6fff3` |
| Spark / master | 4.0.2 / `local[4]` |
| Connector | `spark-sql-kafka-0-10_2.13:4.0.2` |
| Spark limit | 4 CPUs / 4 GiB; 3-GiB driver |
| Kafka limit | 2 CPUs / 1.5 GiB |
| Trigger | Available Now |
| Maximum offsets per trigger | 25,000 |
| Shuffle partitions | 32 |
| Watermark | 10 minutes on `scheduled_replay_timestamp` |
| Deduplication key | `event_id` |

Before execution, the broker reported low watermark zero for every partition and a high watermark at or beyond every required producer end. New output and checkpoint paths did not exist. The command used the image by digest and did not resolve packages, JARs, or Ivy dependencies at runtime.

| Partition | Required start | Required end | Messages |
|---:|---:|---:|---:|
| 0 | 32,942 | 197,542 | 164,600 |
| 1 | 32,213 | 193,213 | 161,000 |
| 2 | 33,862 | 203,062 | 169,200 |
| 3 | 34,359 | 206,059 | 171,700 |
| 4 | 32,513 | 195,013 | 162,500 |
| 5 | 34,220 | 205,220 | 171,000 |

Spark's observed nonempty partition ranges exactly matched this table.

## First execution reconciliation

| Outcome | Count |
|---|---:|
| Kafka Bronze landed | 1,000,000 |
| Silver accepted | 1,000,000 |
| Rejected quarantine | 0 |
| Duplicate | 0 |
| Unique event IDs | 1,000,000 |
| Unique lineage roots | 10,000 |
| Accepted synthetic rows | 0 |

The equations `input = accepted + rejected + duplicate` and `producer offset delta = landed` passed. Final lag was zero in all three queries. No record was dropped by the watermark.

## Performance, lag, and state

| Metric | Value |
|---|---:|
| Application duration | 1,967.873 s / 32.80 min |
| Logical end-to-end throughput | 508.16 messages/s |
| Maximum reported lag | 167,408 offsets |
| Final lag | 0 |
| Accepted state maximum rows | 144,992 |
| Accepted state maximum memory | 58,680,528 B |
| State rows updated | 1,000,000 |
| State rows removed after watermark advancement | 880,000 |
| Rows dropped by watermark | 0 |

| Query | Source rows | Nonempty batches | Summed batch duration | Internal rate |
|---|---:|---:|---:|---:|
| Bronze landing | 1,000,000 | 41 | 170.536 s | 5,863.85 rows/s |
| Accepted validation/deduplication | 1,000,000 | 41 | 932.684 s | 1,072.17 rows/s |
| Rejected validation | 1,000,000 | 41 | 710.921 s | 1,406.63 rows/s |

The three independent queries intentionally read the same boundary separately. Summed batch durations are internal processing measures and overlap neither Spark startup nor final read-back in the same way as total application duration; they must not be added to claim a separate end-to-end runtime.

## Independent verification

The first independent verifier attempt was preserved as a failed verification attempt. It assumed batch-only derived columns such as `event_date` existed in streaming Silver and failed with Spark `UNRESOLVED_COLUMN` after reading the unchanged output. No data or checkpoint was modified.

The verifier was corrected to require an explicit `batch` or `streaming` schema profile. The successful streaming-profile verification independently proved:

| Check | Result |
|---|---:|
| Rows / unique events / unique sequences | 1,000,000 / 1,000,000 / 1,000,000 |
| Unique detections | 10,000 |
| Detection frequencies | exactly 100 |
| Replay iterations | complete 1-100 |
| Sequence range | 0-999,999 |
| Schedule range | `2026-08-08T00:00:00.000Z` to `2026-08-08T02:46:39.990Z` |
| Source type | 1,000,000 `NASA_REPLAY` |
| Synthetic true / null parents | 0 / 0 |
| Broker-coordinate or streaming-status invalid rows | 0 |
| Verification duration | 255.385 s |
| Verification throughput | 3,915.66 rows/s |

Broker-coordinate parity covers topic, partition, offset, and timestamp. Streaming-status parity covers Spark validation acceptance, empty Spark validation errors, canonical validation acceptance, and unique deduplication status.

## Checkpoint recovery

The second execution reused the exact output and all three original checkpoints.

| Check | Result |
|---|---:|
| New Bronze source rows | 0 |
| New accepted source rows | 0 |
| New rejected source rows | 0 |
| Counts unchanged | Yes |
| Parquet file counts unchanged | Yes |
| Parquet bytes unchanged | Yes |
| Offsets reconciled | Yes |
| Silver truth reverified | Yes |
| Recovery duration including full read-back | 156.178 s |

No checkpoint was deleted, edited, or recreated.

## Storage and resources

| Output | Parquet files | Parquet bytes |
|---|---:|---:|
| Kafka Bronze | 246 | 417,298,297 |
| Silver accepted | 1,312 | 483,632,931 |
| Rejected | 41 | 231,404 |

- Total streaming output directory including checksum/metadata files: 909,809,939 bytes.
- Checkpoint directory: 161,079,529 bytes.
- Free D: disk after execution and recovery: 988,006,920,192 bytes.
- Peak observed Spark memory: 3.600 GiB / 4 GiB, 90%.
- Peak observed Kafka memory during this gate: approximately 514.9 MiB / 1.5 GiB.
- Final Kafka data directory: 1,580,958,241 bytes including prior history and engine files.
- Actual cloud cost: USD 0.00.
- All 54 automated tests passed.

The 1,312 Silver files average approximately 369 KiB. This is a severe small-file result created by 32 shuffle partitions across 41 stateful microbatches and makes downstream Gold compaction mandatory.

## Limitations

- Three independent queries reread and parse the same Kafka boundary, prioritizing checkpoint isolation over minimum compute.
- Sampled resource observations are not continuous peak telemetry.
- The laptop run does not prove distributed Spark executor behavior, broker replication, failover, authentication, or concurrent producers.
- One original NASA observation date remains represented; event-date partition diversity is unmeasured.
- The near-cap Spark memory sample and small-file count prohibit using this local configuration for a five-million streaming claim.
- Zero natural rejected or duplicate events occurred; bounded fixtures cover those branches.
- The failed independent-verifier attempt is an evidence-tool contract error, not a streaming data failure.

## Gate decision

Phase 6G.4 passes. The one-million streaming Silver output is trustworthy but is not selected for direct Gold consumption because its 1,312 tiny files are intentionally preserved as raw streaming evidence. Phase 6G.5 will build governed Gold from the already admitted 32-file one-million batch Silver output, which contains the same replay truth at a more appropriate downstream file boundary. Kafka, Spark, and PostgreSQL are stopped; Kafka and checkpoint evidence are preserved.
