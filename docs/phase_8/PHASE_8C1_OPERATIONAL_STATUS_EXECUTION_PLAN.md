# Phase 8C.1 - Operational Status API Execution Plan

## Status

Approved explicitly as the prerequisite to Phase 8C dashboard implementation. No dashboard work is included.

## Endpoint

`GET /v1/platform/status` is the only new route. It accepts no parameters and returns one bounded response assembled from existing governed sources:

- the latest successful PostgreSQL Gold/load reconciliation through a safe read-only serving view;
- the latest and last-successful Phase 7 Airflow immutable run manifests;
- bounded cache configuration and aggregate entry count from the active cache backend;
- the existing API and pipeline versions.

The endpoint exposes no paths, cache keys or values, credentials, SQL, infrastructure settings, secrets, or Airflow configuration.

## Permission boundary

Migration `005` creates a one-row operational projection in the existing `serving` schema and grants only `SELECT` to `eo_api_readonly`. The API login retains forced read-only transactions and cannot query underlying `load_control` tables directly.

## Bounds and failures

- At most 1,000 immutable orchestration manifests may be inspected.
- The database view returns at most one row.
- Unknown query parameters fail validation.
- Missing, malformed, absent-success, or over-bound Airflow metadata returns a safe 503 response.
- Existing database error handlers remain unchanged.

## Verification

1. Unit tests verify composition, schema rejection, latest-versus-last-successful semantics, safe failure mapping, parameter rejection, and sensitive-field exclusion.
2. Live integration verifies the one-million database projection, runtime role, underlying-table denial, endpoint reconciliation, and latency.
3. Existing API, cache, Airflow, and complete regression suites remain green.
4. Runtime evidence stays outside Git, temporary services stop, and the verified milestone is committed independently.
