# Data Dictionary

## Status

Version 1 baseline. Field definitions will be expanded only when their implementation milestone begins.

## Identity and lineage

| Field | Definition |
|---|---|
| `event_id` | Unique identity of one processed event message |
| `detection_id` | Stable identity of the underlying Earth Observation detection |
| `lineage_root_id` | Original detection identity from which a replay or enrichment derives |
| `source_type` | `NASA_ORIGINAL`, `NASA_REPLAY`, or `SYNTHETIC_SCALE_TEST` |
| `source_dataset` | Dataset or product identifier governing the source record |
| `source_record_id` | Deterministic identity generated from approved immutable source attributes |
| `is_synthetic` | True only for explicitly synthetic scale-test records |
| `ingestion_run_id` | Unique identity of the ingestion or generation run |
| `replay_run_id` | Controlled replay run identifier |
| `synthetic_generation_id` | Synthetic generation run identifier |

## Time

| Field | Definition |
|---|---|
| `event_timestamp` | UTC time at which the source observation or synthetic event occurred |
| `ingestion_timestamp` | UTC time at which the platform first received the record |
| `processing_timestamp` | UTC time at which Spark processed the record into Silver |
| `kafka_timestamp` | Kafka record timestamp when the streaming path is used |

## Location and measurements

| Field | Definition |
|---|---|
| `latitude` | Detection latitude in decimal degrees |
| `longitude` | Detection longitude in decimal degrees |
| `bright_ti4_kelvin` | VIIRS I4 brightness temperature in kelvin |
| `bright_ti5_kelvin` | VIIRS I5 brightness temperature in kelvin |
| `fire_radiative_power_mw` | Fire radiative power in megawatts |
| `scan_km` | Along-scan footprint dimension in kilometers |
| `track_km` | Along-track footprint dimension in kilometers |
| `confidence` | NASA source confidence category |
| `day_night` | Day (`D`) or night (`N`) observation indicator |
| `satellite` | Source satellite identifier |
| `instrument` | Source instrument identifier |
| `source_product_version` | NASA source product version |

## Governance

The canonical event contract under `contracts/events/v1/` is authoritative for required fields, types, compatibility, classification, and validation invariants.

