# Governed Gold 1,000,000-Message Gate

## Result

**Status:** Passed

**Gate:** Phase 6G.5

**Admitted Gold run:** `80083cae-e529-462f-bf06-f7576bbc1ecc`

**Pipeline revision:** `a597fa4`

**Gold contract:** `1.1.0`

Exactly one million accepted `NASA_REPLAY` messages representing 10,000 underlying original NASA detections were read from the admitted 32-file Phase 6G.2 batch Silver output. They are replay messages, not one million original NASA observations, and contain zero synthetic records.

## Source boundary

| Item | Value |
|---|---|
| Silver processing run | `b3b54428-dcac-486e-b9ec-7a495fb27f8e` |
| Silver rows | 1,000,000 |
| Silver Parquet files | 32 |
| Silver Parquet bytes | 214,918,941 |
| Source type | `NASA_REPLAY` |
| Underlying NASA detections | 10,000 |
| Replay factor | 100 |

The 1,312-file streaming Silver output remains preserved as streaming evidence but was not used for Gold because the admitted batch Silver represents the same replay truth at a compact 32-file boundary.

## Contract change

Gold contract 1.1 permits multiple newline-delimited JSON load parts. Every load part records exact bytes, SHA-256, and row count, and the PostgreSQL loader fails before staging unless declared part rows sum to the expected manifest count. Version 1.0 single-part manifests remain readable.

The manifest records requested Spark partitions separately from observed physical data-file counts. Spark `coalesce` may produce fewer files than requested when the input has fewer physical partitions; requested partitions are therefore never reported as observed files.

## Preserved non-admitted attempt

The first immutable execution used revision `3e80b9d` and Gold run `33110672-1469-4c63-b401-75d7a2e56625`. It reconciled one million Gold and load rows in 189.193 seconds, but its manifest labeled the requested value of 16 as the partition count while Spark physically produced four Parquet and four JSON parts.

The artifacts were not modified or deleted. The attempt was not admitted because its physical file-count metadata was inaccurate. Revision `a597fa4` separated requested partitions from observed file counts and added a fail-closed physical-artifact presence check before the corrected execution.

## Admitted build reconciliation

| Metric | Value |
|---|---:|
| Input Silver rows | 1,000,000 |
| Gold Parquet read-back | 1,000,000 |
| Load JSON read-back | 1,000,000 |
| Source-type count | 1,000,000 `NASA_REPLAY` |
| Synthetic rows | 0 |
| Build duration | 167.959 seconds |
| Build throughput | 5,953.83 rows/second |
| Requested / observed Gold parts | 4 / 4 |
| Requested / observed load parts | 4 / 4 |
| Manifest SHA-256 | `43ada13e40f14ffcdbd93d76702ee0d5918be7a666235a902375a860b491ffb9` |

The existing loader independently opened and hashed every manifest artifact before database work. It found ten recorded artifacts, four JSON load parts, and exactly 1,000,000 declared load rows.

## Storage

| Product | Data files | Bytes |
|---|---:|---:|
| Gold event-detail Parquet | 4 | 181,977,884 |
| PostgreSQL load JSON | 4 | 2,269,335,690 |
| Complete admitted run directory | - | 2,470,467,522 |

The partitioned load boundary replaces one forecast 2.2-2.5-GB JSON object with four checksum-addressed parts. Full generated artifacts remain outside Git.

## Independent truth verification

The existing independent Spark batch-profile verifier read only the admitted Gold Parquet output using Spark 4.0.2 on `local[4]` with 32 shuffle partitions.

| Check | Result |
|---|---:|
| Rows / unique events / unique sequences | 1,000,000 / 1,000,000 / 1,000,000 |
| Unique detections | 10,000 |
| Events per detection | exactly 100 |
| Replay sequence | complete 0-999,999 |
| Replay iterations | complete 1-100 |
| Schedule | `2026-08-08T00:00:00.000Z` to `2026-08-08T02:46:39.990Z` |
| Source type | 1,000,000 `NASA_REPLAY` |
| Synthetic true / null parents | 0 / 0 |
| Invalid batch-profile rows | 0 |
| Verification duration | 19.951 seconds |
| Verification throughput | 50,121.99 rows/second |

## Runtime and resource evidence

- Spark image: `apache/spark:4.0.2-python3` pinned to `sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3`.
- Runtime: four CPUs, 4-GiB container limit, 3-GiB driver memory, 256-MiB driver result limit.
- Highest sampled memory during the admitted build: 1.946 GiB / 4 GiB.
- The admitted container exited zero and was not OOM-killed.
- Free D: disk after build and verification: 983,066,230,784 bytes.
- Actual cloud cost: USD 0.00.
- All 59 automated tests passed.
- Spark, Kafka, and PostgreSQL are stopped after evidence capture.

Resource values are bounded samples rather than continuous peak telemetry.

## Limitations

- This gate creates event-detail Gold and the governed database load boundary; PostgreSQL loading and aggregate/database truth remain Phase 6G.6.
- The local bind mount and mechanical D: drive affect runtime and are not cloud performance measurements.
- All records represent one original NASA observation date, so multi-date partition behavior remains unmeasured.
- The verifier reuses the established batch-profile truth logic because Gold preserves all admitted Silver identity, lineage, replay, schedule, and processing fields.
- The non-admitted attempt remains on disk and is explicitly excluded from Phase 6G.6.

## Gate decision

Phase 6G.5 passes. Only manifest `data/local/gold/gate_count=1000000/gold_run_id=80083cae-e529-462f-bf06-f7576bbc1ecc/manifest.json` is admitted for Phase 6G.6. The next milestone is a clean compact PostgreSQL/PostGIS rebuild from that checksum-validated manifest.
