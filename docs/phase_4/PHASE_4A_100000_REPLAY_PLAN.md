# Phase 4A - Deterministic 100,000-Record Replay Plan

## Status

Design complete. Replay implementation and data generation require owner approval.

## Objective

Create a truthful, deterministic plan for the 100,000-record scale gate without claiming 100,000 unique NASA observations and without generating additional geospatial measurements.

## Design Choice

Use controlled replay of the admitted 10,000 original NASA events with a replay factor of 10.

The resulting gate contains:

- 100,000 event messages
- 10,000 unique underlying NASA detections
- 0 newly fabricated NASA observations
- 0 synthetic events
- 100,000 explicitly classified `NASA_REPLAY` events

Controlled replay is preferred over synthetic generation at this gate because it exercises message identity, lineage, deterministic scheduling, Kafka partitioning, deduplication, and streaming semantics while preserving real NASA measurement values. Synthetic scale-test generation remains appropriate for later gates when replay alone no longer provides sufficient distribution or volume control.

## Generation Flow

```text
Admitted 10,000 NASA originals
        |
        v
Verify count + SHA-256 + classifications
        |
        v
Sort originals by event_id
        |
        v
Apply deterministic replay plan x10
        |
        +--> new replay event_id
        +--> preserved detection and raw lineage
        +--> replay iteration and sequence
        +--> deterministic scheduled replay timestamp
        |
        v
100,000 replay JSONL messages
        |
        v
Manifest + checksum + reconciliation
        |
        v
Independent repeat generation
        |
        v
Approval before Spark/Kafka execution
```

## Identity and Lineage

The versioned `nasa-replay-v1` identity hashes the deterministic replay run ID, original parent event ID, and replay iteration. Every replay gets a unique event-message ID while retaining:

- original `detection_id`
- original `lineage_root_id`
- original `source_record_id`
- original source ingestion run
- original NASA event timestamp
- original platform ingestion timestamp
- original measurements and raw-object lineage

This permits both event-message metrics and unique-detection metrics without conflation.

## Deterministic Schedule

The logical schedule begins at `2026-08-07T00:00:00.000Z`, assigns a 10-millisecond interval, and spans 999.99 seconds. These timestamps support reproducible ordering and later event-time streaming tests.

They are scheduled timestamps, not performance claims. Kafka broker timestamps and observed producer throughput will record what actually happened.

## Kafka Compatibility

The planned replay topic is `eo.events.replay.v1` with six local partitions. Kafka keys use `lineage_root_id`, ensuring that all ten replay messages for one NASA detection remain on one partition and retain lineage order.

Stable event identity remains in `event_id` for deduplication. Rejected and dead-letter topics remain physically separate.

Kafka deployment, client dependencies, topic creation, and publication are outside Phase 4A.

## Estimated Data Volume

Estimates are derived from the measured 10,000-record artifacts and include replay-field overhead. They are planning ranges, not measured results.

| Artifact | Estimated size |
|---|---:|
| 100,000 replay JSONL | 170-190 MB |
| Silver Parquet | 40-55 MB |
| Generation manifest and quality evidence | Below 1 MB |
| Kafka payload plus local topic/index overhead | 200-300 MB |
| Temporary, quarantine, and repeat-run allowance | 300-500 MB |
| Recommended free working headroom | At least 2 GB |

The replay generator must stream output instead of holding 100,000 event dictionaries in memory. It may hold and sort the 10,000-record source set, which is already proven manageable.

## Laptop Execution Envelope

- Generate with Python standard library; add no dependency.
- Keep Kafka stopped during artifact generation.
- Keep Spark stopped during artifact generation.
- Store generated data only under ignored `data/local/` paths.
- Run replay generation twice sequentially, not concurrently.
- For the later Spark gate, retain the 4-CPU, 3-GB container limit initially.
- Begin with 12-16 shuffle partitions for 100,000 records only after measuring file sizes; do not tune without evidence.
- Keep at least 2 GB of disk headroom beyond expected artifacts and logs.

## Required Tests

### Identity tests

- equivalent replay inputs produce the same event ID
- replay iteration changes event ID
- parent event changes event ID
- replay plan changes replay run and event IDs

### Lineage tests

- detection, lineage-root, source-record, measurement, and raw-lineage fields are preserved
- replay classification is always `NASA_REPLAY`
- synthetic flag is always false
- parent IDs resolve to source originals
- NASA observation timestamps remain unchanged

### Ordering and schedule tests

- sequence covers 0 through 99,999 without gaps or duplicates
- output sequence is strictly ascending
- scheduled timestamps are monotonic and exactly 10 ms apart
- each replay iteration contains all 10,000 parents once

### Failure tests

- source checksum mismatch stops before output
- source count mismatch stops before output
- non-original source rows are rejected
- duplicate source event IDs invalidate the run
- partial output is never presented as successful
- fewer or more than 100,000 outputs fail reconciliation

### Repeatability tests

- two complete generations produce byte-identical event JSONL
- two output SHA-256 values match
- manifests distinguish physical execution runs while referencing the same deterministic replay run

## Performance Evidence

The generation run must record:

- source and output bytes
- source and output counts
- unique events and detections
- runtime and records per second
- replay classifications and synthetic flags
- first and last sequence and schedule timestamps
- output SHA-256
- pipeline revision
- laptop limitations

The later Spark run must preserve the existing accepted/rejected/duplicate reconciliation and Parquet read-back requirements.

## Risks and Controls

| Risk | Control |
|---|---|
| Recruiters interpret replay messages as original observations | State both 100,000 replay messages and 10,000 unique NASA detections in every report. |
| Replay overwrites observation time | Preserve `event_timestamp`; use a separate `scheduled_replay_timestamp`. |
| Reruns create different identities | Derive logical replay run and event IDs only from versioned deterministic inputs. |
| Actual Kafka speed is confused with schedule | Record broker timestamps and measured throughput separately. |
| Memory grows with output size | Stream JSON Lines; never build the 100,000-record output list. |
| One corrupted source propagates ten times | Verify source checksum, count, uniqueness, schema, and classification before generation. |
| Kafka ordering is assumed globally | Guarantee order only within the stable `lineage_root_id` key partition. |
| Mechanical disk slows repeated generation | Run sequentially and record wall-clock performance as a laptop limitation. |

## Phase 4B Implementation Boundary

After approval, Phase 4B may:

1. implement `nasa-replay-v1` identities and streaming JSONL generation
2. extend the canonical Spark schema with replay fields
3. add unit and failure tests
4. generate exactly 100,000 replay events twice
5. reconcile counts, lineage, classifications, sequence, and checksums
6. record generation performance and limitations
7. stop before Spark processing or Kafka deployment

Phase 4B may not install Kafka clients, start Kafka, publish messages, run the 100,000-record Spark gate, or generate synthetic observations without separate approval.

## Phase 4A Completion Criteria

- replay classification and truthfulness rules are explicit
- deterministic plan and event identity algorithms are defined
- original observation and ingestion timestamps are preserved
- scheduled replay time is separate from actual Kafka time
- Kafka topic and key compatibility are defined
- volume and laptop risks are estimated from measured evidence
- required tests and reconciliations are specified
- no replay dataset or dependency has been created
- owner approval is required before implementation

