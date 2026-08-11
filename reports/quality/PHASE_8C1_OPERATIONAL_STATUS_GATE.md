# Phase 8C.1 Operational Status API Gate

## Result

**PASSED** on 2026-08-10 against the preserved one-million-row PostgreSQL/PostGIS database and verified Phase 7 immutable Airflow metadata.

## Endpoint contract

Exactly one route was added: `GET /v1/platform/status`. It accepts no parameters and returns:

- last successful pipeline completion;
- latest Airflow run ID and status;
- latest governed Gold manifest ID, SHA-256, and contract version;
- cache enabled state, TTL, and aggregate entry count;
- API and pipeline versions;
- latest successful database-load freshness;
- reconciled quality-gate status.

The explicit response schema rejects invalid hashes, negative cache counts, nonpositive TTLs, invalid statuses, missing fields, and extra fields. Unknown request parameters return 422.

## Source and permission verification

- Gold, manifest, pipeline version, freshness, and quality fields came from the latest successful database load through `serving.platform_operational_status`.
- The one-row view returned quality `PASSED` only because staged rows equaled manifest rows and inserted plus already-present rows equaled manifest rows.
- `eo_api_runtime` retained `transaction_read_only=on` and membership only through `eo_api_readonly`.
- Direct `SELECT` from `load_control.database_load_run` failed with insufficient privilege.
- Migration `005` has SHA-256 `e07e704a632b2c5c1be3a416efd605c64fbfcbb72758b40003aab2f71cae015a`.
- Airflow fields came from the existing immutable Phase 7 run manifest set under a maximum of 1,000 manifests.
- Cache status came from the active backend and exposed no keys or values.

No response contains filesystem paths, cache keys, cache values, database credentials, SQL, Airflow secrets, infrastructure configuration, or internal implementation details.

## Failure evidence

The first live attempt failed during application startup because `pathlib.Path` collided with FastAPI's existing `Path` helper. No endpoint or database query executed. Aliasing the filesystem type corrected only that compatibility defect. A subsequent assertion expected Gold version `1.1`, while the governed database correctly returned `1.1.0`; the test was synchronized to the authoritative metadata without changing production behavior.

Missing directories, empty metadata sets, malformed JSON, invalid status fields, absent successful runs, and sets over 1,000 manifests fail closed. The endpoint maps these conditions to `503 {"detail":"operational metadata unavailable"}` without returning internal errors.

## Latency

Profile: 30 warm, sequential, in-process TestClient requests.

| Metric | Value |
|---|---:|
| p50 | 38.396 ms |
| p95 | 52.392 ms |
| p99 | 54.174 ms |

Raw benchmark JSON remains ignored at `data/local/api/phase8c1_status_benchmark.json`. This is not a network, concurrency, or cloud measurement.

## Tests

- Focused API suite: 18 passed.
- Complete default discovery: 91 discovered, 83 passed, 8 intentional environment-specific skips.
- Live PostgreSQL integration: 5 passed.
- Official Airflow Linux image: 3 passed.
- Across admitted environment-specific executions: 91 passed, 0 failed.

## Milestone boundary

Phase 8C.1 is independently verified. Dashboard implementation did not begin.
