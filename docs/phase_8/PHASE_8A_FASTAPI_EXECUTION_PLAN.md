# Phase 8A - FastAPI Read-Only Serving Execution Plan

## Status

Approved by the owner's explicit Phase 8A instruction. The Phase 6 database architecture and Phase 7 orchestration behavior remain unchanged.

## Objective

Implement one read-only FastAPI application over the existing compact PostgreSQL/PostGIS serving projection. The API exposes only health/readiness, platform summary, activity-time daily aggregates, detection lineage, and bounded GeoJSON bounding-box search.

## Runtime and dependency boundary

- FastAPI 0.139.2 and Uvicorn 0.51.0 on Python 3.12.
- Existing psycopg 3.3.4 driver; no ORM, pool, cache, authentication product, or additional service.
- Pydantic request and response models through FastAPI.
- One connection per request in Version 1.0, bounded by PostgreSQL connection and statement timeouts.
- Database DSN supplied only by `EO_API_DATABASE_DSN`; it must authenticate as the non-owner `eo_api_runtime` login.

## Endpoint contract

| Endpoint | Bound |
|---|---|
| `GET /health/ready` | one constant database probe; verifies current role and read-only transaction state |
| `GET /v1/summary` | one summary row; optional source/dataset/activity-time filters, maximum 31-day range |
| `GET /v1/daily` | maximum 200 aggregate rows, maximum 31-day range |
| `GET /v1/lineages/{lineage_root_id}` | seek pagination, default 50 and maximum 100 events |
| `GET /v1/events/bbox` | seek pagination, default 100 and maximum 500 GeoJSON features, maximum 7-day activity range |

All event-oriented time filtering uses `coalesce(scheduled_replay_timestamp, event_timestamp)` as activity time. Observation time remains separately labeled as `event_timestamp` in detail responses.

## Security and SQL

- Migration `004` creates the non-owner `eo_api_runtime` login without a committed password, grants membership in `eo_api_readonly`, and forces read-only transactions plus a fifteen-second statement timeout. It adds one measured activity-time B-tree access path for bounded replay-time filtering and seek pagination; the existing PostGIS GiST index remains the spatial access path.
- The runtime rejects owner/superuser roles and requires `transaction_read_only=on` before readiness succeeds.
- Every client value is passed as a psycopg parameter. SQL text is selected only from application-owned constants.
- No endpoint accepts arbitrary identifiers, ordering, SQL fragments, raw paths, or internal payloads.
- Unknown query parameters are rejected. Coordinates, time ranges, cursors, limits, and source classifications are validated before SQL executes.

## Geospatial contract

The bounding-box query requires `min_longitude < max_longitude` and `min_latitude < max_latitude`; antimeridian-crossing boxes are rejected in Version 1.0. It applies both the PostGIS `geometry && ST_MakeEnvelope(...)` predicate and exact `ST_Intersects`, preserving the existing SRID-4326 GiST access path. Responses are GeoJSON FeatureCollections with Point coordinates in longitude/latitude order.

## Verification

1. Unit tests validate schemas, bounds, cursor behavior, source truth, response filtering, SQL parameter separation, and database-error mapping.
2. Integration tests run all endpoints against the preserved one-million-row database through `eo_api_runtime`.
3. Database permission probes prove SELECT succeeds and INSERT/UPDATE/DELETE/DDL fail.
4. `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` proves the spatial query uses `event_detail_geometry_gist_idx`.
5. Fixed warm workloads record p50, p95, and p99 endpoint latency.
6. All prior tests pass, runtime state remains outside Git, services stop, evidence is committed, and the working tree is clean.
