# NASA Controlled Replay Contract - Version 1

## Purpose

Define deterministic replay event messages derived from governed original NASA events. Replay increases event-message volume for pipeline testing without fabricating additional NASA observations.

## Truthful Classification

Every replay event must have:

- `source_type = NASA_REPLAY`
- `is_synthetic = false`
- the original `source_dataset`
- a non-empty deterministic `replay_run_id`
- a non-empty `parent_event_id`
- the original `detection_id`, `lineage_root_id`, and `source_record_id`

A replay event is not an original NASA observation and is not a synthetic observation. Reporting must distinguish:

- 100,000 replay event messages
- derived from 10,000 unique original NASA detections
- replay factor of 10

## Input Contract

Version 1 replay accepts only the admitted 10,000-record input with:

- source type `NASA_ORIGINAL`
- synthetic flag `false`
- unique `event_id` values
- version 1 canonical schema
- input SHA-256 `3f711f63138ae1e5c926d5dcd6edf3a15094ae8f7d8631db4e566b936862ede3`
- exactly 10,000 records

The generator must verify the input checksum and record count before producing output.

## Replay Plan

| Parameter | Version 1 value |
|---|---|
| Identity algorithm | `nasa-replay-v1` |
| Source records | 10,000 |
| Replay factor | 10 |
| Output messages | 100,000 |
| Ordering | Replay iteration ascending, then original `event_id` ascending |
| First iteration | 1 |
| Last iteration | 10 |
| First sequence number | 0 |
| Last sequence number | 99,999 |
| Scheduled replay start | `2026-08-07T00:00:00.000Z` |
| Scheduled interval | 10 milliseconds |
| Logical schedule duration | 999.99 seconds |

The logical schedule controls deterministic event content. It does not claim that Kafka actually emitted messages at the scheduled rate. Actual producer timestamps, throughput, and lag must be measured separately.

## Deterministic Replay Run Identity

Build canonical replay-plan JSON with sorted keys and compact separators from:

- identity algorithm
- source input SHA-256
- source record count
- replay factor
- scheduled replay start
- scheduled interval milliseconds
- ordering algorithm

Calculate SHA-256 over its UTF-8 bytes:

`replay_run_id = nasa-replay-v1:sha256:<plan_sha256>`

Equivalent plans produce the same logical replay run identity. Wall-clock execution identity belongs in the generation manifest and must not alter event bytes.

## Deterministic Event Identity

For each original event and replay iteration, build canonical identity JSON with sorted keys and compact separators from:

- `identity_algorithm = nasa-replay-v1`
- `replay_run_id`
- `parent_event_id`
- `replay_iteration`

Calculate SHA-256 over its UTF-8 bytes:

`event_id = nasa-replay-v1:sha256:<event_sha256>`

The combination of replay run, parent event, and iteration must be unique. Changing these semantics is a breaking contract change.

## Field Transformations

| Field | Replay rule |
|---|---|
| `event_id` | New deterministic replay message identity |
| `parent_event_id` | Original NASA `event_id` |
| `detection_id` | Preserve original |
| `lineage_root_id` | Preserve original |
| `source_record_id` | Preserve original |
| `source_type` | Set to `NASA_REPLAY` |
| `source_dataset` | Preserve original |
| `is_synthetic` | Set to `false` |
| `ingestion_run_id` | Preserve original source ingestion run |
| `replay_run_id` | Set to deterministic replay-plan identity |
| `event_timestamp` | Preserve original NASA observation time |
| `ingestion_timestamp` | Preserve original platform ingestion time |
| `scheduled_replay_timestamp` | Replay start plus sequence number multiplied by 10 ms |
| `replay_iteration` | Integer from 1 through 10 |
| `replay_sequence_number` | Unique integer from 0 through 99,999 |
| NASA measurements | Preserve original values exactly |
| raw lineage | Preserve original raw object, row, and payload hash |
| Kafka metadata | Null before Kafka publication |
| validation status | Reset to pending replay validation before Spark admission |
| deduplication status | Reset to pending before Spark admission |

Generation metadata such as execution UUID, wall-clock start, wall-clock completion, runtime, and output checksum belongs in the replay manifest, not in deterministic event content.

## Output Ordering

Let original events be sorted by ascending `event_id` with zero-based index `i`. For replay iteration `r` from 1 through 10:

`replay_sequence_number = ((r - 1) * 10000) + i`

`scheduled_replay_timestamp = schedule_start + (replay_sequence_number * 10 milliseconds)`

Output JSON Lines in ascending `replay_sequence_number`. A repeated generation must produce byte-identical event output and the same SHA-256.

## Storage Layout

```text
data/local/bronze/replay_events/
  source_type=NASA_REPLAY/
    replay_plan_version=1/
      replay_run_id_sha256=<deterministic-id-digest>/
        events.jsonl

data/local/manifests/replay/
  run_date=<yyyy-mm-dd>/
    <execution-run-id>.json
```

Generated replay data remains outside Git. The repository may contain only compact representative fixtures and evidence.

## Required Manifest Fields

- execution run ID
- deterministic replay run ID
- status
- identity algorithm
- pipeline revision
- source path and checksum
- source count
- replay factor
- expected output count
- actual output count
- unique event ID count
- unique detection ID count
- source-type counts
- synthetic-flag counts
- first and last sequence numbers
- first and last scheduled replay timestamps
- output path, byte count, and SHA-256
- start, completion, duration, and throughput
- reconciliation status
- failure category when unsuccessful

## Reconciliation Invariants

- `output_count = source_count * replay_factor = 100000`
- `unique_event_id_count = 100000`
- `unique_detection_id_count = 10000`
- each `detection_id` occurs exactly 10 times
- all `source_type` values are `NASA_REPLAY`
- all `is_synthetic` values are `false`
- all parent event IDs exist in the admitted source input
- sequence numbers are complete and unique from 0 through 99,999
- scheduled timestamps are monotonic in output order
- repeated output SHA-256 values match

Failure of any invariant invalidates the generation run.
