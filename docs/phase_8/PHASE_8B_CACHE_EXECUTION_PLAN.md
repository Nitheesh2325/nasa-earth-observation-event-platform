# Phase 8B - Bounded Replaceable Cache Execution Plan

## Status

Approved by the owner's explicit Phase 8B instruction. Phase 8A endpoint, validation, response, database-role, and SQL contracts remain unchanged.

## Boundary

- Cache only successful `GET /v1/summary` and `GET /v1/daily` responses after request and response validation.
- Never cache readiness, lineage, bounding-box detail, invalid requests, database errors, or failed response validation.
- Keep the backend byte-oriented and injected at application creation so a later replacement does not alter routes or schemas.
- Use the standard `Cache-Control: no-cache` or `no-store` request directive for safe bypass.

## Fixed local policy

| Control | Bound |
|---|---:|
| TTL | 60 seconds |
| Maximum entries | 256 |
| Maximum serialized entry | 65,536 bytes |
| Maximum serialized cache memory | 4,194,304 bytes |
| Eviction | least recently used |

Keys are versioned SHA-256 digests of canonical JSON generated only from validated Pydantic query models. Namespace separation prevents equivalent parameters on different aggregate endpoints from colliding.

## Failure behavior

Cache read, decode, and write failures are availability optimizations only: the request falls through to the existing read-only PostgreSQL repository. A PostgreSQL or response-validation failure retains Phase 8A error semantics and is never inserted into the cache. Bypass requests neither read nor write cache state.

## Verification

1. Unit tests prove hit, miss, expiration, bypass, deterministic keys, entry/count/byte bounds, LRU eviction, backend failure, PostgreSQL fallback, and uncached endpoint exclusions.
2. The existing API and full default regression suites remain green.
3. Airflow contract tests run in the isolated supported Linux environment.
4. Live integration runs against the preserved one-million-row PostgreSQL/PostGIS database through `eo_api_runtime`.
5. Thirty-sample cache-hit and bypass latency is recorded without changing public API responses.
6. Temporary services stop and runtime evidence stays outside Git.
