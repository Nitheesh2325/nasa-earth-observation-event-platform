# Kafka Rejected and Dead-Letter Routing Gate

## Result

**Passed** on 2026-08-07. One invalid replay message was routed to `eo.events.rejected.v1`, one valid message exhausted exactly three controlled processing attempts and was routed to `eo.events.dlq.v1`, and one valid control message was not routed.

This is a deterministic bounded fault fixture. It does not claim a real external dependency failed.

## Runtime identity

| Item | Value |
|---|---|
| Implementation revision | `5b6dcc9` |
| Kafka | 4.3.1, pinned image digest |
| Python/native client | confluent-kafka/librdkafka 2.15.0 |
| Routing envelope version | 1.0.0 |
| Maximum processing attempts | 3 |
| Fault injection | Explicit test-only CLI flag |
| Automated tests | 40 passed |

## Source fixture

| Item | Value |
|---|---|
| Fixture SHA-256 | `03271749ed81aaacee06e7e5119e59e57208e4d4c49a7256524b6a4297eb3e80` |
| Producer run ID | `ab98e4bf-7178-4077-b4b4-c4acc6ac714e` |
| Attempted / acknowledged / offset delta | 3 / 3 / 3 |
| Delivery failures | 0 |

The fixture contains one valid control event, one replay event with a missing event ID and latitude 91, and one otherwise valid replay event marked for controlled retry exhaustion.

## Routing reconciliation

Routing run ID: `1d876fdc-66b3-4304-9a13-71de3d765e11`.

| Outcome | Count |
|---|---:|
| Source consumed | 3 |
| Valid control passed | 1 |
| Rejected published | 1 |
| DLQ published | 1 |
| Delivery failures / unflushed | 0 / 0 |
| Router duration | 0.269 seconds |

The source reconciliation is `3 = 1 + 1 + 1`.

| Topic | Start offsets | End offsets | Delta |
|---|---|---|---:|
| `eo.events.rejected.v1` | `[0,0,0]` | `[0,0,1]` | 1 |
| `eo.events.dlq.v1` | `[0,0,0]` | `[0,0,1]` | 1 |

Both keys hashed to partition 2. Partition choice is not asserted; exact offset deltas and stable lineage keys are asserted.

## Rejected envelope verification

- Kafka key: `route-invalid-lineage`
- routing type: `REJECTED`
- processing attempts: 0
- reason codes: `INVALID_LATITUDE`, `MISSING_EVENT_ID`
- original source: replay topic, partition 2, offset 33,861
- original event and UTF-8 value preserved
- envelope version, key, and unique source coordinate verified independently

## Dead-letter envelope verification

- Kafka key: `route-fault-lineage`
- event ID: `route-fault-event`
- routing type: `DLQ`
- processing attempts: exactly 3
- reason code: `PROCESSING_RETRIES_EXHAUSTED`
- bounded failure category: `RuntimeError`
- original source: replay topic, partition 5, offset 34,219
- original event and UTF-8 value preserved
- envelope version, key, and unique source coordinate verified independently

## Resource observation

After routing, Kafka used approximately 469.7 MiB of its 1.5-GiB limit and 2.34% CPU in the bounded snapshot. These values are not peak telemetry or a throughput benchmark.

## Safety and limitations

- Fault injection is deterministic, fixture-only, and requires an explicit CLI flag.
- The router validates the bounded routing contract in Python; Spark validation and Parquet quarantine remain independently tested.
- The fixture does not prove backoff timing, a real external outage, multi-instance concurrency, or transactional atomicity across input and output topics.
- Local topics use replication factor one and plaintext localhost networking.
- No PostgreSQL, AWS, or million-record work occurred.
