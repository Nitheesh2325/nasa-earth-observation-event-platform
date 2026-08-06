# Kafka 100,000-Message Replay Gate

## Result

**Passed** on 2026-08-06. Exactly 100,000 controlled `NASA_REPLAY` messages derived from 10,000 unique original NASA detections were published and diagnostically reconciled. These are replay messages, not 100,000 new NASA observations.

## Governed input

| Item | Value |
|---|---|
| Source artifact | Deterministic `nasa-replay-v1` JSONL |
| Source SHA-256 | `9380341108650b2a5b536f9245148abf572883eb7b13ba0c332d0583fb5e0b0a` |
| Artifact bytes | 184,078,310 |
| Messages | 100,000 |
| Unique NASA lineage roots | 10,000 |
| Classification | `NASA_REPLAY`, `is_synthetic=false` |
| Pipeline revision | `d56394b` |

The producer verified the complete source checksum before requesting broker watermarks or sending messages.

## Runtime contract

| Item | Value |
|---|---|
| Broker | Apache Kafka 4.3.1, single-node local KRaft |
| Broker image digest | `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| Python/native client | confluent-kafka/librdkafka 2.15.0 |
| Topic | `eo.events.replay.v1` |
| Topic partitions | 6 |
| Replication factor | 1, local-only |
| Message key | UTF-8 `lineage_root_id` |
| Producer guarantees | idempotence, `acks=all`, bounded retries, delivery callbacks |

## Producer reconciliation

Producer run ID: `3b82e8fe-9b0b-4ad5-b12d-bd21eef1d90a`.

| Check | Result |
|---|---:|
| Attempted | 100,000 |
| Acknowledged | 100,000 |
| Delivery failures | 0 |
| Undelivered after flush | 0 |
| Total offset delta | 100,000 |
| Serialized value bytes | 183,978,310 |
| Duration | 11.8245 seconds |
| Throughput | 8,457.01 records/second |

The JSONL artifact is 100,000 bytes larger than the serialized Kafka values because each source record has one newline delimiter that is removed before publication.

| Partition | Start offset | End offset | Delivered |
|---:|---:|---:|---:|
| 0 | 20 | 16,480 | 16,460 |
| 1 | 12 | 16,112 | 16,100 |
| 2 | 21 | 16,941 | 16,920 |
| 3 | 19 | 17,189 | 17,170 |
| 4 | 13 | 16,263 | 16,250 |
| 5 | 18 | 17,118 | 17,100 |

The partition counts total exactly 100,000. Every partition received messages. Counts range from 16,100 to 17,170 around a mean of 16,666.67.

## Diagnostic reconciliation

Diagnostic run ID: `0edb6af9-9b29-404e-b351-4f508c7133b6`.

The consumer was manually assigned the producer manifest's exact start offsets and stopped after the corresponding exact end offsets. It did not rely on auto-commit state.

| Check | Result |
|---|---:|
| Expected / consumed | 100,000 / 100,000 |
| Unique event IDs | 100,000 |
| Duplicate messages | 0 |
| Invalid JSON values | 0 |
| Missing event IDs | 0 |
| Kafka key/lineage mismatches | 0 |
| Validation duration | 2.9734 seconds |
| Validation throughput | 33,631.75 records/second |

Consumed partition counts exactly matched delivered partition counts.

## Resource observation

The post-run broker snapshot showed approximately 523.7 MiB memory usage out of the 1.5-GiB limit and 183% CPU on Docker's per-core percentage scale, within the two-CPU cap. This snapshot is not an average or peak profile.

## Completion assessment

- Input checksum matched the admitted artifact.
- Attempted, acknowledged, offset-delta, consumed, and unique-event counts all reconciled to 100,000.
- All diagnostic quality counters were zero.
- Stable-key distribution exercised all six partitions without material skew for this input.
- Local manifests remain outside Git; compact truthful evidence is committed.
- Kafka was stopped after evidence capture and its persistent volume was retained.
- Spark Structured Streaming was not started.

## Limitations

This local one-broker run does not demonstrate replication, broker failover, TLS, authentication, authorization, multi-producer contention, consumer-group rebalancing, sustained lag, or end-to-end Spark processing. The producer rate must not be extrapolated directly to one million or ten million records.
