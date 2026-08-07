# Kafka 1,000,000-Message Replay Gate

## Result

**Status:** Passed  
**Gate:** Phase 6G.3  
**Producer run:** `41ab9e88-27ab-4936-9e9c-fa7ea181dfcd`  
**Diagnostic run:** `3a120763-8122-49c2-9592-eb7ff79975af`  
**Publication revision:** `18bc065`  
**Fail-closed consumer revision:** `70e2876`

Exactly one million controlled `NASA_REPLAY` messages representing 10,000 underlying original NASA detections were published to a fresh, explicitly recorded Kafka offset boundary and independently consumed only from that boundary. These are replay messages, not one million original NASA observations.

## Governed input

| Item | Value |
|---|---|
| Physical replay execution | `3224f997-d2a8-4494-a1a1-e771b4804739` |
| Source SHA-256 | `67d32855fe894c6b4e2a5237045f25db4374762320d6a561dc0d438efa2e7778` |
| Artifact bytes | 1,842,603,090 |
| Messages | 1,000,000 |
| Underlying NASA detections | 10,000 |
| Replay factor | 100 |
| Classification | `NASA_REPLAY`, `is_synthetic=false` |

The producer checksum-validated the entire admitted artifact before reading broker watermarks or sending messages.

## Runtime contract

| Item | Value |
|---|---|
| Broker | Apache Kafka 4.3.1, single-node local KRaft |
| Image digest | `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| Client | confluent-kafka/librdkafka 2.15.0 |
| Topic | `eo.events.replay.v1` |
| Partitions | 6 explicit partitions |
| Replication | 1, local-only |
| Key | UTF-8 `lineage_root_id` |
| Producer | idempotence, `acks=all`, Zstandard compression, bounded retries, delivery callbacks |
| Consumer | manual assignment, exact offsets, no auto commit/store, fail-closed truth checks |

The replay, rejected, and dead-letter topic metadata matched the committed contracts before publication. A transient `Coordinator load in progress` occurred while acquiring the producer ID; the bounded idempotent client retry succeeded. It is retained as operational evidence and did not produce a delivery failure.

## Producer reconciliation

| Check | Result |
|---|---:|
| Attempted | 1,000,000 |
| Acknowledged | 1,000,000 |
| Delivery failures | 0 |
| Undelivered after flush | 0 |
| Broker offset delta | 1,000,000 |
| Serialized values excluding newlines | 1,841,603,090 B |
| Duration | 64.367 s |
| Throughput | 15,536.03 messages/s |

| Partition | Start | End | Delivered | Difference from mean |
|---:|---:|---:|---:|---:|
| 0 | 32,942 | 197,542 | 164,600 | -1.24% |
| 1 | 32,213 | 193,213 | 161,000 | -3.40% |
| 2 | 33,862 | 203,062 | 169,200 | +1.52% |
| 3 | 34,359 | 206,059 | 171,700 | +3.02% |
| 4 | 32,513 | 195,013 | 162,500 | -2.50% |
| 5 | 34,220 | 205,220 | 171,000 | +2.60% |

All six partitions were used. The distribution is exactly ten times the measured 100,000-run distribution because both artifacts use the same 10,000 stable lineage keys with proportionally larger replay frequency.

## Independent bounded consumption

| Check | Result |
|---|---:|
| Expected / consumed | 1,000,000 / 1,000,000 |
| Per-partition counts equal producer | Pass |
| Unique event IDs | 1,000,000 |
| Duplicate messages | 0 |
| Invalid JSON | 0 |
| Missing event IDs | 0 |
| Kafka key/lineage mismatches | 0 |
| Unique detections | 10,000 |
| Events per detection | exactly 100 |
| Replay sequence | complete 0-999,999 |
| Replay iterations | complete 1-100 |
| `NASA_REPLAY` messages | 1,000,000 |
| Synthetic true | 0 |
| Duration | 26.066 s |
| Throughput | 38,364.39 messages/s |

The prior diagnostic implementation only made offset totals part of success. During this gate it was strengthened before consumption so every recorded quality and truth condition is required for `SUCCEEDED`; any nonzero malformed, duplicate, key mismatch, truth mismatch, detection-frequency mismatch, or incomplete sequence/iteration check now fails the run.

## Resource and storage evidence

- Publication snapshot: 494.9 MiB / 1.5 GiB, 66.70% CPU, 202 MB read / 66.4 MB written block I/O.
- Post-consumption snapshot: 546.4 MiB / 1.5 GiB, 2.04% CPU, 202 MB read / 243 MB written block I/O.
- Kafka data directory after the gate: 1,580,538,375 bytes, including prior retained topic history, indexes, metadata, and allocated segments.
- Host free disk before start: 989,099,839,488 bytes.
- Actual cloud cost: USD 0.00.
- Kafka was stopped after evidence capture; its named volume is preserved for Phase 6G.4.
- PostgreSQL remained stopped and no Spark container ran during this subgate.
- All 54 automated tests passed.

## Limitations

- One local broker with replication factor one does not prove broker high availability, failover, TLS, authentication, authorization, or multi-broker durability.
- Producer and diagnostic consumer ran sequentially, not under concurrent sustained ingestion and processing pressure.
- CPU and memory are sampled snapshots, not continuous peak telemetry.
- Topic retention is bounded. Phase 6G.4 must verify that every recorded start offset is still available before Structured Streaming begins.
- The stable-key balance result applies to this 10,000-lineage population and should not be generalized to arbitrary source distributions.
- The Kafka data-directory size is not payload size because it includes prior history, indexes, metadata, compression, and segment allocation.

## Gate decision

Phase 6G.3 passes. The producer manifest and exact start/end offsets are admitted as the only source boundary for Phase 6G.4 Structured Streaming. Republish is prohibited unless offset retention has invalidated the boundary; that condition must be recorded as a failed/expired boundary rather than hidden. Kafka is stopped with its named volume preserved.
