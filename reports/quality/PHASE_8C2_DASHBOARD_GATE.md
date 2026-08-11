# Phase 8C.2 Version 1.0 Dashboard Gate

## Result

**PASSED** on 2026-08-10 against the live FastAPI layer and preserved one-million-row compact PostgreSQL/PostGIS database.

## Architecture boundary

The Streamlit 1.61.1 dashboard calls only:

- `GET /health/ready`
- `GET /v1/platform/status`
- `GET /v1/summary`
- `GET /v1/daily`
- `GET /v1/events/bbox`
- `GET /v1/lineages/{lineage_root_id}`

The dashboard package has no psycopg import, SQL, database DSN, filesystem metadata read, production response fixture, or copied aggregation logic. Pydantic models from the FastAPI package validate every response. The API origin is supplied by `EO_DASHBOARD_API_BASE_URL`.

## Verified presentation

- Mission Overview: readiness, quality, last successful pipeline run, API version, platform version, and data freshness.
- Pipeline Overview: original, replay, synthetic, total messages, Gold version, manifest identity, and Airflow status.
- Daily Activity: interactive 31-day maximum aggregate chart with source classification filter.
- Geospatial Explorer: ordered coordinate validation, seven-day maximum activity window, source filter, 50/100/250/500-point bounds, classification colors, zoom, and tooltip.
- Detection Lineage: validated ID search, truth counts, source dataset/record, replay sequence, observation time, activity time, classification, and parent event.
- Health Panel: readiness, cache enabled state, TTL, entries, latest Airflow run, and quality gate.

All displayed values originated from FastAPI. Static UI labels, bounds, and source colors are presentation controls rather than data claims.

## State verification

| State | Result |
|---|---|
| Initial loading controls for every remote section | passed |
| Successful one-million rendering | passed |
| Daily empty result | passed |
| Map initial and empty results | passed |
| API connection failure | passed |
| HTTP error and invalid response | passed |
| Invalid base URL and lineage ID | passed |
| Maximum 200-row daily response | passed |
| Bounded map filter submission | passed |
| Detection-lineage search and table | passed |

The first Streamlit test attempt exposed a test-harness import-context limitation and did not execute production code. An explicit test entry wrapper corrected it. The first real screenshot exposed truncated values in six- and seven-column metric rows; responsive two-row card layouts corrected the presentation before screenshots were admitted.

## Live truth verification

- Event messages: 1,000,000 `NASA_REPLAY`.
- Underlying NASA detections: 10,000.
- Original messages: 0.
- Synthetic messages: 0.
- Gold contract: 1.1.0.
- Quality gate: PASSED.
- Airflow: SUCCEEDED.
- Map: 250 bounded replay points rendered from GeoJSON.
- Lineage: 100 replay events rendered with source and activity semantics.

## Browser performance

Ten overview reload-to-daily-chart-ready samples were 985, 1,236, 1,562, 1,725, 1,729, 1,733, 1,748, 1,827, 2,105, and 2,500 ms. Median was 1,731 ms and p95 was 2,500 ms. The 250-point map interaction completed in 2,079 ms. The 100-event lineage interaction completed in 311 ms.

At a 900x800 viewport, document width and client width both measured 900 pixels and all first eight mission/pipeline metrics retained their full values. Screenshots were admitted at the default 1280x720 desktop viewport.

## Tests

- Focused dashboard suite: 9 passed.
- Complete default discovery: 100 discovered, 92 passed, 8 intentional environment-specific skips.
- Isolated official Airflow image: 3 passed.
- Dependency integrity: no broken requirements.
- Dashboard SQL/database boundary scan: clean.

## Screenshot evidence

- `docs/images/dashboard-overview-v1.png`
- `docs/images/dashboard-geospatial-v1.png`
- `docs/images/dashboard-lineage-v1.png`

## Limitations

- Measurements are local sequential browser interactions, not hosted, concurrent, WAN, or cloud results.
- Map display is capped at 500 points per request and lineage at 100 events per response.
- Streamlit process memory and multi-user concurrency require deployment-phase evidence.
- Public authentication and hosting are not part of Phase 8C.2.
