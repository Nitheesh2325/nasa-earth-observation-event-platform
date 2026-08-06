# NASA 10,000-Record Input Gate

## Result

**Status:** Passed

**Date:** 2026-08-06

This report covers source acquisition, canonical reconciliation, and deterministic input selection. It is not yet a Spark Bronze-to-Silver performance result.

## Source Availability and Request

The official NASA FIRMS availability endpoint reported `VIIRS_SNPP_SP` availability from 2012-01-20 through 2026-04-27. The fixed historical date 2026-04-01 was selected within that range.

| Property | Value |
|---|---|
| Source type | `NASA_ORIGINAL` |
| Source dataset | `VIIRS_SNPP_SP` |
| Area | `world` |
| Day range | 1 |
| Historical date | 2026-04-01 |
| HTTP attempts | 1 |
| HTTP result | 200 |
| Ingestion run ID | `bf743c8e-0a6c-408c-aae8-2a44cdaceefa` |
| Source records | 44,292 |
| Raw bytes | 3,466,282 |
| Raw SHA-256 | `40386d30b04efd4ff32e001d59f8b3fccda20b6e2d878b9f598322bb29a9734e` |

The raw response and its secret-safe manifest remain under ignored local Bronze storage.

## Canonical Reconciliation

| Metric | Count |
|---|---:|
| Input | 44,292 |
| Accepted | 44,292 |
| Rejected | 0 |
| Duplicates | 0 |
| Reconciled | Yes |

Canonicalization run ID: `afdcbc76-36dd-43e4-b66a-f975cbf04d8d`.

## Deterministic Selection

The `event-id-ascending-v1` algorithm sorted accepted unique events by stable `event_id` and selected the first exactly 10,000 records.

| Property | Value |
|---|---|
| Selection run ID | `dff1d6f2-7b26-48dc-9068-b3d51cd3ed64` |
| Pre-selection records | 44,292 |
| Selected records | 10,000 |
| Selected bytes | 15,227,942 |
| Selected SHA-256 | `3f711f63138ae1e5c926d5dcd6edf3a15094ae8f7d8631db4e566b936862ede3` |
| Selected source type | `NASA_ORIGINAL` |
| Synthetic records | 0 |
| Reconciled | Yes |

An independent repeat selection used run ID `d54d2a96-709c-45a4-82c9-cf36236d08e4` and produced the identical selected-event SHA-256.

## Automated Verification

- 23 repository tests passed.
- Selector tests cover stable ordering, repeat-output checksum equality, and honest failure when input is too small.
- Source, canonical, and selected artifacts are excluded from Git.
- No secret is present in committed evidence.

## Limitations

- Event-ID ordering creates a deterministic engineering benchmark; it is not a statistically representative scientific sample.
- This gate does not claim Spark transformation throughput.
- Spark container performance measurement begins only after the Bronze-to-Silver job and its explicit schema are approved and implemented.
- All selected records are NASA-derived original observations from the fixed FIRMS response; later replay and synthetic records must remain separately labeled.

## Gate Decision

The exact 10,000-record input is admitted to the first Spark Bronze-to-Silver batch gate. Implementation must read it with an explicit schema, preserve lineage, quarantine invalid records, deduplicate by the governed stable key, write partitioned Parquet, read it back, and record runtime and throughput.

