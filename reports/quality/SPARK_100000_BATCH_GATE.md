# Spark 100,000-Message Bronze-to-Silver Gate

## Result

**Status:** Passed

**Processing run ID:** `9630a4bb-901e-42f5-a4ed-9eae854120ed`

## Truthful Input Statement

The input contains 100,000 controlled `NASA_REPLAY` event messages derived from 10,000 unique original NASA detections. It does not contain 100,000 original NASA observations and contains no synthetic events.

Input SHA-256: `9380341108650b2a5b536f9245148abf572883eb7b13ba0c332d0583fb5e0b0a`.

## Reconciliation

| Outcome | Before write | Parquet read-back |
|---|---:|---:|
| Accepted Silver | 100,000 | 100,000 |
| Rejected quarantine | 0 | 0 |
| Duplicate quarantine | 0 | 0 |
| Total input | 100,000 | 100,000 reconciled outcomes |

The governing equation `100,000 = 100,000 + 0 + 0` passed. All output datasets were read back before success was recorded.

## Environment

| Property | Value |
|---|---|
| Spark image | `apache/spark:4.0.2-python3` |
| Image digest | `sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3` |
| Container limit | 4 CPUs, 3 GiB RAM |
| Driver memory | 2 GiB |
| Master | `local[4]` |
| Shuffle partitions | 16 |
| Default parallelism | 4 |
| Pipeline revision | `baca172` |

## Performance and Output

| Metric | Value |
|---|---:|
| Duration | 61.232 seconds |
| Throughput | 1,633.13 records/second |
| Silver Parquet files | 16 |
| Silver Parquet bytes | 38,399,832 |
| Event-date partitions | 1 |
| Silver schema fields | 50 |

All Silver records remain under `event_date=2026-04-01`, preserving original NASA observation time rather than replacing it with scheduled replay time.

## Independent Silver Verification

| Invariant | Result |
|---|---|
| Rows | 100,000 |
| Unique replay event IDs | 100,000 |
| Unique original detection IDs | 10,000 |
| Unique sequence numbers | 100,000 |
| Sequence range | 0-99,999 |
| Replay iteration range | 1-10 |
| First scheduled replay | `2026-08-07T00:00:00.000Z` |
| Last scheduled replay | `2026-08-07T00:16:39.990Z` |
| Valid `NASA_REPLAY`, non-synthetic classification | 100,000 |
| Non-null parent lineage | 100,000 |
| Required derived Silver fields | Present |

## Comparison With 10,000 Gate

| Metric | 10,000 gate | 100,000 gate |
|---|---:|---:|
| Input messages | 10,000 | 100,000 |
| Duration | 36.029 s | 61.232 s |
| Throughput | 277.56/s | 1,633.13/s |
| Silver bytes | 3,913,774 | 38,399,832 |
| Parquet files | 8 | 16 |

The larger gate achieved higher observed throughput because fixed Spark startup and reconciliation costs were amortized over more records. This single comparison does not prove linear scalability or justify extrapolation to one million records.

## Limitations

- Both gates contain one original NASA observation date, so multi-date partition scaling remains unmeasured.
- The 100,000 gate uses deterministic replay and therefore repeats the original measurement distribution ten times.
- Docker bind mounts and the mechanical D: drive affect local results.
- Count-distinct verification was executed separately and is not included in the recorded job duration.
- Zero real rejected or duplicate messages occurred; the committed integration fixture covers those branches.
- Kafka producer, broker, consumer, lag, and Structured Streaming performance remain unmeasured.

## Gate Decision

The 100,000-message Spark batch gate is complete. Kafka work requires a separate design and dependency approval milestone.

