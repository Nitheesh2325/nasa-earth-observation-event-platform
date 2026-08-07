# Kafka Replay Topic Contract - Version 1

## Scope

Define governed replay, rejection, and dead-letter messages for the local Kafka and Structured Streaming platform.

## Version Pins

- broker image: `apache/kafka:4.3.1`, with digest recorded after approved pull
- Python producer and diagnostic consumer: `confluent-kafka==2.15.0`
- Spark Kafka connector: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2`

## Topics

| Topic | Purpose | Local partitions | Local replication | Production replication |
|---|---|---:|---:|---:|
| `eo.events.replay.v1` | Valid replay event messages | 6 | 1 | 3 |
| `eo.events.rejected.v1` | Contract-invalid messages with reason metadata | 3 | 1 | 3 |
| `eo.events.dlq.v1` | Messages that exhaust bounded processing retries | 3 | 1 | 3 |

Topics must be created explicitly in KRaft mode. Automatic topic creation is prohibited.

Local replay-topic retention is 24 hours with a 128-MiB per-partition cap and 64-MiB segments. Rejected and dead-letter retention is seven days with a 64-MiB per-partition cap and 32-MiB segments. Local maximum message size is 2 MiB.

## Message Key

Use UTF-8 `lineage_root_id` as the Kafka message key.

This keeps every replay derived from the same original detection on one partition and preserves per-lineage publication order. `event_id` remains the unique message identity used for deduplication; it is not the partition key.

## Message Value

- UTF-8 canonical JSON
- canonical event schema version `1.0.0`
- replay contract version `1`
- no credential or secret
- no Kafka offset, partition, or broker timestamp before publication

Serialization must be deterministic for an equivalent event. The producer must not mutate the governed replay artifact in place.

## Producer Requirements

- acknowledgments: `all`
- idempotent producer: enabled
- bounded retry count and bounded delivery timeout
- compression: `zstd`, subject to measured compatibility
- idempotence: enabled
- `acks=all`
- final flush remainder must be zero
- stable ordering within each lineage key
- record successes, failures, retries, bytes, duration, and throughput
- never report scheduled replay rate as achieved throughput

## Consumer Requirements

- explicit consumer group IDs
- manual or controlled offset commits after successful processing
- track consumer lag
- preserve topic, partition, offset, and Kafka timestamp in downstream records
- route invalid messages to `eo.events.rejected.v1`
- route exhausted processing failures to `eo.events.dlq.v1`
- use checkpoints and watermarks in Structured Streaming where appropriate

## Failure-routing envelope

Rejected and dead-letter values use routing envelope version `1.0.0` and retain:

- routing type and physical routing run ID
- routing timestamp
- original source topic, partition, offset, and Kafka timestamp
- original message key
- event ID and lineage root where parseable
- stable reason codes
- processing attempt count
- bounded failure category
- parsed original event where available
- original UTF-8 value for diagnosis

The output Kafka key remains `lineage_root_id`. Rejected messages have contract or key-integrity errors and use zero processing attempts. Dead-letter messages passed contract validation but exhausted the configured bounded processing attempts.

Production code must never infer a retry count that did not occur. Test fault injection requires an explicit CLI flag and a committed fixture marker; it is not enabled by default in any service configuration.

## Retention and Laptop Controls

The first local test will use replication factor 1 and bounded retention. Kafka and Spark must not compete for the laptop's full memory allocation. Retention, segment size, producer batch size, and message rate require a later measured infrastructure plan.

The approved starting envelope is 1.5 GiB and two CPUs for Kafka, with a 768-MiB maximum JVM heap. Spark retains its 3-GiB and four-CPU container cap. Unrelated containers remain stopped during measurement.
