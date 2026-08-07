# Phase 5E - Full Structured Streaming Gate Plan

## Status

Architecture and execution contract approved for design only. The 100,000-message Structured Streaming execution requires separate owner approval.

## Objective

Process exactly one fresh publication of the admitted 100,000-message `NASA_REPLAY` artifact through the pinned derived Spark-Kafka image while proving source-boundary, checkpoint, watermark, deduplication, output, and restart reconciliation.

## Runtime identity

- base Spark image: pinned Spark 4.0.2 digest
- connector: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2`
- derived image: `sha256:d92fdb4dc4cc1febc451308ea17880f48b511f65528cc792120a2345b9d6fff3`
- no runtime `--packages`, `--jars`, Ivy resolution, or network dependency
- Kafka: pinned 4.3.1 image, six replay partitions
- Spark: `local[4]`, four CPUs, 3 GiB container memory, 2 GiB driver memory
- Kafka: two CPUs, 1.5 GiB container memory

## Fresh source boundary

1. Start the preserved broker and verify topic configuration.
2. Record current replay-topic end offsets.
3. Republish the checksum-admitted 100,000-message artifact once using a new producer run ID.
4. Require 100,000 acknowledgements, zero failures, and an exact offset delta of 100,000.
5. Use that producer manifest's per-partition start offsets as Spark `startingOffsets`.
6. Allow no concurrent publication until all three Spark queries finish.
7. Require each Spark query's final end offsets to equal the producer manifest's end offsets.

Existing topic history is outside the gate because the explicit producer boundary excludes it.

## Streaming queries

| Query | Input boundary | Output | State |
|---|---|---|---|
| Kafka Bronze landing | Producer start to end offsets | Append-only Parquet with Kafka metadata and raw bytes | Dedicated checkpoint |
| Accepted Silver | Same range | Parsed, valid, unique Parquet | Ten-minute watermark plus `event_id` state |
| Rejected quarantine | Same range | Invalid event Parquet with reason codes | Dedicated checkpoint |

Use `scheduled_replay_timestamp` for watermarking. Preserve `event_timestamp` as the original NASA observation time. Use an Available Now trigger and an initial `maxOffsetsPerTrigger=10000`; this is a controlled finite streaming gate, not a continuously running service benchmark.

## Required reconciliation

- producer attempted = acknowledged = broker offset delta = 100,000
- Bronze landed = 100,000
- accepted + rejected + duplicate = landed
- expected real-artifact outcome: accepted 100,000, rejected 0, duplicate 0
- Silver Parquet read-back = accepted
- rejected read-back = rejected
- all observed Kafka ranges equal the producer manifest
- all six partitions are represented
- source type is `NASA_REPLAY` for every accepted row
- `is_synthetic=false` for every accepted row
- unique event IDs = 100,000
- unique lineage roots = 10,000
- Kafka key equals `lineage_root_id` for every message
- restart with the same checkpoints consumes zero new rows and does not change output counts

Any mismatch stops the gate. Counts must never be adjusted manually.

## Evidence

Record:

- Git revision and all image digests
- producer and streaming execution IDs
- source checksum and exact offsets
- per-query Spark progress JSON
- microbatch counts, durations, input rate, processing rate, and lag
- watermark and state-store metrics
- accepted, rejected, duplicate, and read-back counts
- Parquet files and bytes by outcome
- Docker CPU and memory snapshots for Kafka and Spark
- total wall-clock runtime with timing boundaries
- checkpoint restart evidence
- limitations and actual local cost

Local manifests, checkpoints, Kafka data, connector binaries, and full Parquet outputs remain outside Git. Only compact evidence is committed.

## Failure and recovery rules

- Never reuse the fixture checkpoint for the full gate.
- Never delete a failed checkpoint to convert a failure into success.
- Preserve a failed execution manifest before retrying.
- A retry may reuse the same checkpoint only when query identity, schema, source boundary, and sink paths are unchanged.
- A new logical query requires a new streaming run ID, checkpoint root, and output root.
- No rejected-topic or dead-letter claims are allowed until those paths have independent tests.

## Completion criteria

- derived image starts the connector without runtime dependency resolution
- fresh 100,000-message producer boundary reconciles
- all three queries terminate successfully and exactly match that boundary
- counts and Parquet read-backs reconcile
- checkpoint restart processes zero new input
- performance and quality evidence is committed
- Kafka and Spark are stopped

## Explicit exclusions

- no million-record scale advancement
- no AWS deployment
- no PostgreSQL or dashboard work
- no rejected-topic producer or dead-letter implementation
- no claim of broker high availability or end-to-end exactly-once across arbitrary external sinks
