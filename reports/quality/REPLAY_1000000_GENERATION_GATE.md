# Deterministic 1,000,000-Message Replay Generation Gate

## Result

**Status:** Passed  
**Gate:** Phase 6G.1  
**Execution date:** 2026-08-07  
**Generation revision:** `338bc2c4fb65`  
**Independent verifier revision:** `4f5518d`

Two immutable physical executions produced byte-identical controlled replay artifacts. An independent streaming verifier then parsed the complete first artifact and recomputed every event identity, schedule position, lineage mapping, preserved NASA field, truth classification, byte count, and SHA-256.

This artifact contains one million controlled replay event messages representing 10,000 underlying original NASA detections. It does not contain or claim one million original NASA observations.

## Logical plan

| Field | Value |
|---|---|
| Source artifact count | 10,000 admitted NASA-original events |
| Source SHA-256 | `3f711f63138ae1e5c926d5dcd6edf3a15094ae8f7d8631db4e566b936862ede3` |
| Replay factor | 100 |
| Expected output | 1,000,000 |
| Schedule start | `2026-08-08T00:00:00.000Z` |
| Interval | 10 milliseconds |
| Replay run ID | `nasa-replay-v1:sha256:4c2894719b188baf5526fe8fcf84256535847e17558ff2e92dafd314789ba149` |

## Physical executions

| Metric | Execution 1 | Execution 2 |
|---|---:|---:|
| Execution run ID | `3224f997-d2a8-4494-a1a1-e771b4804739` | `e029ee3c-78d5-4bc0-a8db-d355aacad56a` |
| Output rows | 1,000,000 | 1,000,000 |
| Output bytes | 1,842,603,090 | 1,842,603,090 |
| Duration | 55.911 s | 49.777 s |
| Throughput | 17,885.72 rows/s | 20,089.41 rows/s |
| Output SHA-256 | `67d32855fe894c6b4e2a5237045f25db4374762320d6a561dc0d438efa2e7778` | identical |

Physical execution IDs and manifest timestamps differ by design. The logical plan identity and governed event bytes are identical.

## Independent full-artifact verification

| Check | Result |
|---|---:|
| Parsed rows | 1,000,000 |
| Recomputed unique event IDs | 1,000,000 |
| Unique detection IDs | 10,000 |
| Events per detection | exactly 100 |
| `NASA_REPLAY` rows | 1,000,000 |
| `NASA_ORIGINAL` rows | 0 |
| Synthetic-true rows | 0 |
| Replay sequence | exactly 0-999,999 in order |
| Replay iterations | exactly 1-100 |
| Parent, detection, lineage, and source-record mapping | Pass |
| Preserved original NASA fields | Pass |
| First scheduled timestamp | `2026-08-08T00:00:00.000Z` |
| Last scheduled timestamp | `2026-08-08T02:46:39.990Z` |
| Recomputed bytes | 1,842,603,090 |
| Recomputed SHA-256 | `67d32855fe894c6b4e2a5237045f25db4374762320d6a561dc0d438efa2e7778` |
| Verification duration | 53.154 s |
| Verification throughput | 18,813.13 rows/s |

The verifier is separate from the generator's reconciliation loop. It loads and checksum-validates the admitted 10,000-row source, derives the logical replay identity from the manifest plan, validates each generated field against its deterministic position, rejects any changed original field or unexpected field set, and hashes the raw output stream.

## Resource and operational evidence

- Free D: disk before generation: 993,002,221,568 bytes.
- Free D: disk after two immutable artifacts: 989,316,972,544 bytes.
- Disk consumed by two artifacts and their filesystem overhead is consistent with two 1,842,603,090-byte files.
- PostgreSQL stayed stopped with exit code 0.
- Kafka stayed stopped; its older exit code 143 records the prior controlled container stop.
- No Spark container ran during generation or verification.
- Actual cloud cost: USD 0.00; the gate ran locally.
- All 51 automated tests passed after adding verifier coverage.

## Limitations

- This measures local sequential JSON generation and validation on the laptop's mechanical D: drive, not Spark, Kafka, NASA API, or end-to-end throughput.
- The verifier establishes deterministic identity uniqueness from the one-to-one combination of a unique source parent and replay iteration rather than retaining a memory-heavy million-ID set.
- Both complete artifacts remain outside Git. Only this compact evidence and verifier tests are committed.
- Phase 6G.2 must use one admitted physical artifact and may not regenerate data to hide a downstream failure.

## Gate decision

Phase 6G.1 passes. The admitted input for Phase 6G.2 is execution `3224f997-d2a8-4494-a1a1-e771b4804739`, with SHA-256 `67d32855fe894c6b4e2a5237045f25db4374762320d6a561dc0d438efa2e7778`. Spark, Kafka, and PostgreSQL are stopped.
