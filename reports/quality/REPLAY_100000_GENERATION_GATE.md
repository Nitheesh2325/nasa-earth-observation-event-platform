# Deterministic 100,000-Message Replay Generation Gate

## Result

**Status:** Passed

**Run date:** 2026-08-06

## Truthful Scale Statement

The artifact contains 100,000 controlled replay event messages derived from 10,000 unique original NASA detections. It contains no newly collected NASA observations and no synthetic events.

| Classification | Count |
|---|---:|
| `NASA_REPLAY` event messages | 100,000 |
| Unique original NASA detections represented | 10,000 |
| `NASA_ORIGINAL` messages in replay artifact | 0 |
| Synthetic messages | 0 |
| Replay factor per detection | 10 |

## Logical Replay Plan

- Identity algorithm: `nasa-replay-v1`
- Replay run ID: `nasa-replay-v1:sha256:ab99d9c9797e8dddd84c8b085d20e4cf43b8fd78858cd0223a277ffd4ae09556`
- Ordering: replay iteration, then parent event ID
- Source checksum: `3f711f63138ae1e5c926d5dcd6edf3a15094ae8f7d8631db4e566b936862ede3`
- Schedule start: `2026-08-07T00:00:00.000Z`
- Schedule end: `2026-08-07T00:16:39.990Z`
- Interval: 10 milliseconds
- Sequence range: 0 through 99,999
- Pipeline revision: `4f1aa7da535c`

## Physical Executions

| Metric | Execution 1 | Execution 2 |
|---|---:|---:|
| Execution run ID | `64d67ad3-3f60-4a00-a5df-a1bd7c65e048` | `7808816d-b83f-4ffc-9198-7fbdbc435dfe` |
| Output messages | 100,000 | 100,000 |
| Output bytes | 184,078,310 | 184,078,310 |
| Duration | 5.064 seconds | 5.025 seconds |
| Throughput | 19,745.49 records/second | 19,900.14 records/second |
| SHA-256 | `9380341108650b2a5b536f9245148abf572883eb7b13ba0c332d0583fb5e0b0a` | `9380341108650b2a5b536f9245148abf572883eb7b13ba0c332d0583fb5e0b0a` |

The event files are byte-identical. Their physical execution IDs and manifest wall-clock timestamps differ as intended.

## Independent Artifact Scan

| Invariant | Result |
|---|---|
| Output count equals 100,000 | Pass |
| Output bytes match manifest | Pass |
| Output checksum matches manifest | Pass |
| Unique event IDs equal 100,000 | Pass |
| Unique detection IDs equal 10,000 | Pass |
| Every detection occurs exactly 10 times | Pass |
| Sequence 0-99,999 is complete and ordered | Pass |
| All source types are `NASA_REPLAY` | Pass |
| All synthetic flags are false | Pass |
| Every parent resolves to admitted source | Pass |
| First and last scheduled timestamps match plan | Pass |

## Automated Tests

Thirty-two tests passed. Replay coverage includes:

- logical replay identity repeatability
- plan, parent, and iteration identity sensitivity
- lineage and classification preservation
- scheduled ordering
- byte-identical repeat generation
- checksum mismatch failure
- non-original source failure
- duplicate source identity failure
- explicit Spark replay field types

## Failed Attempt Evidence

An initial physical execution stopped before creating an events file because the original descriptive directory layout exceeded the Windows path-length limit. The failure manifest recorded execution `acdf7558-4017-4d65-ab6e-b7603f7e9e21`. The logical replay identity was preserved while physical directory labels were shortened and covered by the full test suite before successful generation.

## Gate Decision

The deterministic 100,000-message replay artifact is admitted as input for a separately approved Spark batch gate. Kafka deployment and publication remain unapproved.

