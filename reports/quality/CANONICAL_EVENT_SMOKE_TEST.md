# Canonical Event Smoke Test

## Outcome

Passed on 2026-08-06.

This test verifies canonical event identity, validation, lineage, deduplication, reconciliation, and deterministic rerun behavior for the approved 21-record NASA extraction. It is not a 10,000-record scale-gate result.

## Input lineage

| Property | Value |
|---|---|
| Source ingestion run | `92cc653f-4c44-4b86-b50c-98f6fcceff98` |
| Source classification | `NASA_ORIGINAL` |
| Source dataset | `VIIRS_SNPP_NRT` |
| Input records | 21 |
| Canonical schema | `1.0.0` |
| Identity version | `nasa-firms-viirs-v1` |
| Pipeline revision | `ea8e9ad3f1dc` |

## Primary canonicalization run

| Property | Value |
|---|---|
| Canonicalization run | `0cb8ce80-253d-4606-ba5f-9f9252f1796e` |
| Status | Succeeded |
| Accepted | 21 |
| Rejected | 0 |
| Duplicate | 0 |
| Reconciled | Yes |
| Unique event IDs | 21 |
| Duration | 0.0359 seconds |
| Observed events per second | 585.68 |

The observed rate describes a 21-row local Python smoke test. It is not a Spark throughput benchmark.

## Contract verification

| Check | Result |
|---|---|
| Mandatory values missing | 0 |
| Lineage invariant failures | 0 |
| Source-classification failures | 0 |
| UTC timestamp failures | 0 |
| Machine-specific raw paths | 0 |
| Canonical event checksum matches | Yes |
| Rejected output checksum matches | Yes |
| Duplicate output checksum matches | Yes |
| Manifest revision matches Git | Yes |
| Generated events excluded from Git | Yes |
| Generated manifest excluded from Git | Yes |

For every accepted original event:

- `event_id = detection_id = lineage_root_id = source_record_id`
- `source_type = NASA_ORIGINAL`
- `is_synthetic = false`
- Event and ingestion timestamps are explicit UTC values.
- Raw object and row lineage are retained.

## Deterministic rerun

The same source manifest was processed a second time as canonicalization run `c0846c17-b9b2-46ec-81b3-095b02ab07c0`.

Both runs produced the same accepted, rejected, and duplicate counts. The accepted canonical JSON Lines objects were byte-identical with SHA-256:

`5b99a74a1b3497f0d033855c5ffeb4ab6b97b1bd55acfc57448996311c6432bd`

## Automated verification

- Python compilation check passed.
- Twenty standard-library tests passed.
- Tests cover extraction, identity normalization, event-time parsing, lineage invariants, validation, quarantine, duplicate detection, output hashes, failed-run manifests, source checksum failure, count mismatch, bounded retry, secret-safe failures, and Git revision detection.

## Limitations

- The input contains only 21 NASA NRT records.
- No invalid or duplicate records occurred in the live source sample; those paths are covered by automated tests.
- The Python canonicalizer is for contract verification and bounded replay preparation. Large-scale canonical processing will use Spark DataFrame APIs.
- Country, administrative region, geohash, and geometry enrichment have not started.
- Replay and synthetic identities have not been implemented.
- Kafka, Spark, Silver, Gold, PostgreSQL, API, dashboard, and AWS execution have not started.

