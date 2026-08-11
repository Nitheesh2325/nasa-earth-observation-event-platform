# Phase 8B Bounded Replaceable Cache Gate

## Result

**PASSED** on 2026-08-10 against the preserved one-million-row compact PostgreSQL/PostGIS database.

## Implemented boundary

| Behavior | Verification |
|---|---|
| Platform summary cached | passed |
| Daily activity aggregate cached | passed |
| Readiness, lineage, and bbox detail excluded | passed |
| Invalid and failed requests excluded | passed |
| Deterministic validated-parameter keys | passed |
| 60-second TTL and expiration | passed |
| 256-entry maximum | passed |
| 65,536-byte entry maximum | passed |
| 4,194,304-byte total maximum | passed |
| LRU eviction | passed |
| `no-cache` / `no-store` bypass | passed |
| Backend failure fallback to PostgreSQL | passed |
| Public FastAPI paths and response schemas unchanged | passed |

The cache backend contract accepts versioned string keys and serialized bytes. Application creation injects the backend, so replacement does not change endpoint parameters, response schemas, SQL, or database permissions. Cache values are inserted only after Pydantic response validation succeeds.

## Integrity and security

- Keys are `astrayan-api:v1:<namespace>:<sha256>` values generated from canonical JSON emitted by validated Pydantic query models.
- Unknown and invalid parameters fail before repository or cache operations.
- Bypass requests neither read nor write cache state.
- Cache read, decode, and write exceptions fall through to the existing repository; PostgreSQL retains its Phase 8A error behavior.
- The live role remained `eo_api_runtime`, a member of `eo_api_readonly`, with `transaction_read_only=on`.
- Live INSERT, UPDATE, DELETE, and CREATE probes remained denied.
- No external service, credential, endpoint, response field, or write capability was added.

## Live one-million-row verification

All four Phase 8A PostgreSQL integration tests passed in 5.287 seconds. They reconciled one million `NASA_REPLAY` event messages to 10,000 NASA detections, preserved activity-time semantics and source truth, exercised pagination, verified GeoJSON bounds, and proved read-only permissions. The bbox plan still used `event_detail_geometry_gist_idx`; measured planning and execution were 9.785 ms and 0.935 ms.

## Performance evidence

Profile: 30 warm, sequential, in-process single-client requests per workload.

| Workload | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|
| Platform summary cache hit | 4.040 | 5.139 | 5.978 |
| Platform summary PostgreSQL bypass | 47.902 | 54.280 | 88.249 |
| Daily activity cache hit | 4.973 | 10.620 | 10.646 |
| Daily activity PostgreSQL bypass | 41.818 | 50.898 | 52.004 |

The benchmark recorded 60 hits, 2 misses, 2 entries, 648 serialized bytes, zero evictions, zero expirations, and zero rejected writes. Cache-hit p95 was 90.53% lower for platform summary and 79.13% lower for daily activity than explicit bypass. Raw benchmark JSON remains ignored under `data/local/api/`.

## Tests

- Focused API/cache unit suite: 13 passed.
- Complete default discovery: 85 discovered, 78 passed, 7 intentional opt-in skips.
- Live PostgreSQL integration: 4 passed.
- Official Airflow Linux image: 3 passed.
- Across the admitted environment-specific runs: 85 tests passed, 0 failed.

## Limitations

- Cache state is process-local, non-durable, and independent per API worker.
- TTL expiration is the only freshness mechanism in Version 1.0; distributed invalidation and warming are outside approved scope.
- Serialized response bytes are bounded; Python object and interpreter overhead are not a byte-exact process-memory claim.
- Measurements are local TestClient results, not network, concurrency, or cloud benchmarks.
