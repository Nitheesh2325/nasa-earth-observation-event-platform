# NASA FIRMS Extraction Smoke Test

## Outcome

Passed on 2026-08-06.

This is a bounded extraction smoke test, not a scale-gate result and not evidence that the complete pipeline is production ready.

## Source request

| Property | Value |
|---|---|
| Source | NASA FIRMS |
| Product | `VIIRS_SNPP_NRT` |
| Geographic boundary | `-119,33,-117,35` |
| Day range | 1 |
| Source classification | `NASA_ORIGINAL` |
| Ingestion run ID | `92cc653f-4c44-4b86-b50c-98f6fcceff98` |
| Pipeline revision | `416459ac6d73` |
| HTTP status | 200 |
| HTTP attempts | 1 |

## Reconciliation

| Measurement | Result |
|---|---:|
| CSV records | 21 |
| Manifest records | 21 |
| Counts match | Yes |
| Raw bytes | 1,835 |
| Manifest bytes match | Yes |
| SHA-256 matches | Yes |
| Missing required source columns | 0 |
| Credential absent from manifest | Yes |
| Raw object excluded from Git | Yes |
| Manifest excluded from Git | Yes |
| Manifest revision matches Git | Yes |

## Runtime

| Measurement | Result |
|---|---:|
| End-to-end extraction duration | 0.363 seconds |
| Observed records per second | 57.779 |

This runtime describes one small network request. It is not a Spark or platform throughput benchmark.

## Safe source profile

| Measurement | Result |
|---|---:|
| Minimum latitude | 33.49339 |
| Maximum latitude | 34.89849 |
| Minimum longitude | -118.94310 |
| Maximum longitude | -117.10171 |
| Minimum fire radiative power | 0.32 MW |
| Maximum fire radiative power | 1.63 MW |
| Observed day/night values | `D`, `N` |
| Observed confidence values | `l`, `n` |

## Automated verification

- Python compilation check passed.
- Eleven standard-library unit and contract tests passed.
- Tests cover request bounds, source allow-listing, raw-byte preservation, record counts, checksums, failed manifests, empty responses, bounded transient retry, secret-safe manifests, secret-safe tracebacks, environment loading, and Git revision detection.

## Limitations

- The request used near-real-time data, which may later be superseded by a standard-processing product.
- Only a small Southern California bounding box and one day were tested.
- The result contains 21 original NASA detection records, not 10,000 records.
- Canonical event identity has not yet been implemented.
- No replay or synthetic records were created.
- Kafka, Spark, Silver, Gold, PostgreSQL, API, dashboard, and AWS execution have not started.
- Network timing from one small request must not be generalized into platform capacity.

