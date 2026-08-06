# Kafka Replay Topic Contract - Version 1

## Scope

Define how controlled NASA replay events will be published after the 100,000-record replay artifact passes its batch-generation gate. This contract does not authorize Kafka deployment or publication.

## Topics

| Topic | Purpose | Local partitions | Local replication | Production replication |
|---|---|---:|---:|---:|
| `eo.events.replay.v1` | Valid replay event messages | 6 | 1 | 3 |
| `eo.events.rejected.v1` | Contract-invalid messages with reason metadata | 3 | 1 | 3 |
| `eo.events.dlq.v1` | Messages that exhaust bounded processing retries | 3 | 1 | 3 |

Topics must be created explicitly in KRaft mode. Automatic topic creation is prohibited.

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

## Retention and Laptop Controls

The first local test will use replication factor 1 and bounded retention. Kafka and Spark must not compete for the laptop's full memory allocation. Retention, segment size, producer batch size, and message rate require a later measured infrastructure plan.

