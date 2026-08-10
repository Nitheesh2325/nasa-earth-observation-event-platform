# Phase 8A FastAPI Read-Only Serving Gate

## Result

**PASSED** on 2026-08-10 against the preserved one-million-row compact PostgreSQL/PostGIS database.

## API surface

| Method and path | Result | Bound |
|---|---|---|
| `GET /health/ready` | Passed | one constant database probe |
| `GET /v1/summary` | Passed | optional approved filters; maximum 31-day activity range |
| `GET /v1/daily` | Passed | inclusive maximum 31-day activity range; maximum 200 rows |
| `GET /v1/lineages/{lineage_root_id}` | Passed | seek pagination; limit 1-100 |
| `GET /v1/events/bbox` | Passed | seek pagination; limit 1-500; maximum 7-day activity range |

OpenAPI contained exactly these five GET-only paths. Every success response references an explicit schema. Unknown parameters, malformed cursors, excessive limits, invalid coordinates, inverted boxes, naive timestamps, and excessive time ranges return safe validation failures.

## Truth and time semantics

- Database events: 1,000,000 controlled `NASA_REPLAY` messages.
- Underlying NASA detections: 10,000.
- Original message count: 0.
- Synthetic message count: 0.
- Activity-time rule: `coalesce(scheduled_replay_timestamp,event_timestamp)`.
- Observation range: 2026-04-01 00:56:00Z to 23:24:00Z.
- Replay activity range: 2026-08-08 00:00:00Z to 02:46:39.990Z.
- Activity aggregate: one 2026-08-08 row, 1,000,000 messages, 1,000,000 events, 10,000 detections, zero synthetic messages.

The existing observation-date aggregate remains unchanged. A distinct activity-date aggregate avoids falsely presenting April observations as April replay activity and prevents unbounded detail aggregation in the API.

## Database security

| Check | Result |
|---|---|
| Runtime role | `eo_api_runtime` |
| Login / superuser / create database / create role | true / false / false / false |
| Member of `eo_api_readonly` | true |
| Default transaction read-only | on |
| Statement timeout | 15 seconds |
| SELECT serving tables | allowed |
| INSERT / UPDATE / DELETE | denied by read-only transaction |
| CREATE | denied by read-only transaction and database privilege |
| Password committed or logged | no |

Migration `004_api_runtime_role.sql` has SHA-256 `030b35e97a052db901343806532b460031c3b02315bdc3564fec56d3f52368e2`. It creates no default password; the integration password was random, session-only, rotated between runs, and never written to the repository.

## Parameterization and bounds

All client values are psycopg parameters. Unit tests passed an SQL-like dataset value and verified it was absent from SQL text and present only in the parameter tuple. Source types are limited to `NASA_ORIGINAL`, `NASA_REPLAY`, and `SYNTHETIC_SCALE_TEST`. Identifiers and ordering are application-owned constants. No endpoint returns raw object paths, hashes, event payloads, internal database errors, or credentials.

Lineage pagination returned disjoint first and second pages using an opaque cursor derived from activity timestamp and event ID. The spatial response was a GeoJSON FeatureCollection with Point coordinates in longitude/latitude order and preserved source type, dataset, event identity, detection identity, replay lineage, observation time, and activity time.

## PostGIS plan

The bounding-box query applies both:

- `geometry && ST_MakeEnvelope(...)` for GiST candidate selection;
- `ST_Intersects(...)` for exact geometry filtering.

The measured exact endpoint predicate used `event_detail_geometry_gist_idx`. Planning time was 8.704 ms and execution time was 0.720 ms. The separate `event_detail_activity_time_idx` supports activity filtering and deterministic seek ordering.

## API latency

Profile: 30 warm, sequential, in-process single-client ASGI requests per endpoint.

| Workload | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|
| Readiness | 30.965 | 47.902 | 52.219 |
| Platform summary | 45.025 | 49.132 | 50.350 |
| Daily activity | 40.422 | 47.720 | 54.693 |
| Detection lineage | 45.800 | 53.968 | 57.464 |
| Spatial bbox | 45.822 | 56.835 | 58.517 |

The ignored raw benchmark evidence is 905 bytes under `data/local/api/`. Measurements include validation, a new local database connection, SQL, response validation, and serialization. They exclude Uvicorn network transport and do not claim concurrency or cloud performance.

The activity aggregate occupied 32,768 bytes, the activity-time seek index occupied 122,724,352 bytes, and the final database size was 1,902,440,931 bytes. The final stopped-service snapshot observed 583.7 MiB of the 2-GiB PostgreSQL limit; it is not a peak-memory claim.

## Failed-attempt evidence

The first integration attempt preserved two failures: the five-second role timeout canceled a cold full-detail platform summary, and the bbox query safely returned a generic server response because the API role lacked PostGIS schema usage. A second attempt exposed the unbounded cost of computing activity-date groups from one million detail rows. The admitted correction:

1. increased the still-bounded statement timeout to 15 seconds;
2. replaced the unfiltered platform scan with existing governed aggregates plus indexed activity boundaries;
3. granted schema usage required to call PostGIS while retaining read-only table privileges;
4. added a loader-maintained activity-date aggregate without changing the Phase 6 observation-date table.

The final integration suite passed all endpoints, permissions, pagination, truth, activity-time, GeoJSON, and GiST checks.

## Tests

- API unit tests: 7 passed.
- Live PostgreSQL integration tests: 4 passed.
- Complete default discovery: 79 tests, 72 passed and 7 intentionally skipped because live PostgreSQL credentials and isolated Airflow are opt-in.
- No test failed in the admitted runs.

## Limitations

- Local TestClient measurements are not external HTTP or concurrency benchmarks.
- One connection per request is the Version 1.0 minimum; pooling belongs to the separately approved caching/deployment boundary and was not added.
- Antimeridian-crossing boxes are rejected rather than split.
- Spatial search is point/bounding-box only and capped at 500 features.
- No authentication, accounts, writes, admin surface, cache, dashboard, AWS deployment, or new dataset was implemented.
