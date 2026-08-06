# Kafka 100-Message Smoke Test

## Result

**Passed** on 2026-08-06. This is a bounded infrastructure and client smoke test, not the 100,000-message scale gate and not a throughput benchmark.

## Reproducibility

| Item | Recorded value |
|---|---|
| Pipeline revision | `3e6494f` |
| Broker | Apache Kafka 4.3.1 |
| Broker image digest | `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| Downloaded image content size | 238,880,881 bytes |
| Installed image disk size | approximately 686 MB |
| Python client | confluent-kafka 2.15.0 |
| Native client | librdkafka 2.15.0 |
| Source artifact SHA-256 | `9380341108650b2a5b536f9245148abf572883eb7b13ba0c332d0583fb5e0b0a` |
| Topic | `eo.events.replay.v1` |
| Stable key | `lineage_root_id` |

## Topic contract verification

Live broker descriptions matched the declared contracts:

| Topic | Partitions | Replication | Retention | Retention bytes per partition | Segment bytes |
|---|---:|---:|---:|---:|---:|
| `eo.events.replay.v1` | 6 | 1 | 86,400,000 ms | 134,217,728 | 67,108,864 |
| `eo.events.rejected.v1` | 3 | 1 | 604,800,000 ms | 67,108,864 | 33,554,432 |
| `eo.events.dlq.v1` | 3 | 1 | 604,800,000 ms | 67,108,864 | 33,554,432 |

All topics use delete cleanup, a 2,097,152-byte maximum message, and minimum in-sync replicas of one. Replication factor one is local-only.

## Diagnostic fixture

The committed three-message fixture SHA-256 is `454b75a38f9efc536591ee9fed61022435af1c7c7740475aec6c5a9c3f255488`. It intentionally contains one repeated event ID and one missing event ID.

| Check | Result |
|---|---:|
| Attempted / acknowledged / offset delta | 3 / 3 / 3 |
| Consumed | 3 |
| Unique event IDs | 1 |
| Duplicate messages | 1 |
| Missing event IDs | 1 |
| Invalid JSON | 0 |
| Key/lineage mismatches | 0 |

Producer run: `009fd223-94fb-4d7a-84c6-f2faf50cd21e`. Diagnostic run: `35ad8009-4883-42c0-b319-0963fb98ef47`.

## Admitted replay sample

Exactly the first 100 messages of the checksum-verified 100,000-message artifact were published. All values are labeled `NASA_REPLAY`, retain NASA lineage, and are not described as 100 new NASA observations.

| Check | Result |
|---|---:|
| Attempted | 100 |
| Broker acknowledgements | 100 |
| Delivery failures / unflushed | 0 / 0 |
| Broker offset delta | 100 |
| Consumed from recorded ranges | 100 |
| Unique event IDs | 100 |
| Duplicate / missing IDs / invalid JSON | 0 / 0 / 0 |
| Key/lineage mismatches | 0 |
| Serialized value bytes | 183,678 |
| Producer duration | 0.642 seconds |
| Observed producer rate | 155.80 records/second |

Partition delivery was `[18, 12, 21, 18, 13, 18]`, totaling 100. Producer run: `1ef8dd30-ad5b-4a31-a27d-b40545db423f`. Diagnostic run: `b32fac7e-1836-40f3-9ff9-581716a99018`.

The recorded rate is startup-dominated and must not be extrapolated to the full gate. The test proves bounded acknowledgement and offset reconciliation.

## Resource observation

During evidence capture, the broker used approximately 410.6 MiB of its 1.5 GiB limit and 3.19% CPU. The persistent volume occupied approximately 1.321 GB after creating 12 partitions and publishing 103 messages. Most of that footprint is Kafka segment preallocation, not event payload.

## Test evidence

The full local standard-library suite passed: 35 tests in 0.412 seconds. Compose configuration validation and the broker health check passed. No full 100,000-message publication and no Spark Structured Streaming job occurred in this milestone.

## Limitation discovered and corrected

The first watermark request failed before publication because the broker advertised `localhost`, which resolved to IPv6 while the host port was IPv4-only. No event was sent in that failed attempt. Revision `3e6494f` advertises `127.0.0.1:9092`; both bounded runs then reconciled successfully.
