# Phase 8C.2 - Version 1.0 Dashboard Execution Plan

## Boundary

Implement one Streamlit 1.61.1 application that calls only the six approved FastAPI GET routes. The dashboard has no database driver use, SQL, local metadata reads, production fixtures, or copied aggregation logic.

## Views

- Overview: mission status, quality, freshness, pipeline truth counts, daily activity, and health/cache/Airflow status.
- Geospatial: validated seven-day bounding-box filters and at most 500 color-classified points from the existing GeoJSON endpoint.
- Detection lineage: validated lineage search and the first bounded 100-event replay chain from the existing lineage endpoint.

## Verification

Unit and Streamlit AppTest coverage verifies validated route use, successful rendering, loading controls, API failure, invalid input, empty results, 200-row daily input, map submission, and lineage search. Final integration uses the preserved one-million API, browser verification, screenshots, and measured load/render evidence before documentation and commit.
