# Canonical Earth Observation Event Contract - Version 1

## Purpose

Define the stable event representation shared by extraction, replay, synthetic generation, Kafka, Spark batch, Spark Structured Streaming, Parquet, PostgreSQL, FastAPI, and the dashboard.

## Classification

| `source_type` | Meaning | `is_synthetic` | Required lineage field |
|---|---|---:|---|
| `NASA_ORIGINAL` | Original NASA detection | `false` | No replay or generation ID |
| `NASA_REPLAY` | Re-emitted event derived from an original NASA detection | `false` | `replay_run_id` |
| `SYNTHETIC_SCALE_TEST` | Artificial scale-test event | `true` | `synthetic_generation_id` |

Enrichment never changes the source classification.

## Required fields

| Field | Logical type | Rule |
|---|---|---|
| `event_id` | string | Unique identity of this event message |
| `detection_id` | string | Stable identity of the underlying detection |
| `lineage_root_id` | string | Identity of the original lineage root |
| `source_type` | enum | One approved classification |
| `source_dataset` | string | Approved NASA or synthetic dataset identifier |
| `source_record_id` | string | Deterministic source identity |
| `is_synthetic` | boolean | Must agree with `source_type` |
| `ingestion_run_id` | string | Identifies the ingestion or generation run |
| `event_timestamp` | UTC timestamp | NASA acquisition time or controlled synthetic event time |
| `ingestion_timestamp` | UTC timestamp | Time first accepted by the platform |
| `latitude` | double | From -90 through 90 |
| `longitude` | double | From -180 through 180 |
| `schema_version` | string | `1.0.0` for this contract |
| `pipeline_version` | string | Version or Git reference of processing logic |

## Conditional fields

| Field | Required when |
|---|---|
| `replay_run_id` | `source_type` is `NASA_REPLAY` |
| `synthetic_generation_id` | `source_type` is `SYNTHETIC_SCALE_TEST` |
| `parent_event_id` | The event is derived from another event message |

## NASA measurement fields

- `bright_ti4_kelvin`
- `bright_ti5_kelvin`
- `fire_radiative_power_mw`
- `scan_km`
- `track_km`
- `confidence`
- `day_night`
- `satellite`
- `instrument`
- `source_product_version`

Measurement fields may be nullable only when the source contract permits absence. Present numeric values must pass the documented plausibility rules before entering Silver.

## Derived Silver fields

- `event_date`
- `event_hour`
- `ingestion_date`
- `geometry_wkt`
- `geohash`
- `country_code`
- `admin1_code`
- `processing_timestamp`
- `validation_status`
- `validation_error_codes`
- `deduplication_status`
- `enrichment_status`

## Operational lineage fields

- `raw_object_uri`
- `raw_file_name`
- `raw_row_number`
- `raw_payload_hash`
- `kafka_topic`
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`

Kafka fields are nullable for batch-only source records.

## Identity rules

- An original event normally uses its deterministic detection identity as the basis of `event_id`.
- A replay receives a unique `event_id` while retaining the original `detection_id` and `lineage_root_id`.
- A synthetic event receives a unique `event_id` and must never impersonate an original NASA source record.
- Message deduplication operates on `event_id`.
- Unique NASA detection reporting operates on `detection_id` with `source_type = NASA_ORIGINAL`.
- Identity generation must be deterministic, versioned, and covered by tests.

For version 1 NASA original events, `event_id`, `detection_id`, `lineage_root_id`, and `source_record_id` are equal to the versioned deterministic source identity. Replay and synthetic identity rules will be added only when those milestones begin.

## Time rules

- All timestamps use UTC.
- Acquisition time must preserve leading zeros before parsing.
- `event_timestamp` must not be replaced with ingestion time when the source event time is available.
- `ingestion_timestamp` must not precede the platform's actual receipt of the record.
- Replay may occur long after the original event time; both times must remain visible.

## Validation outcomes

Each candidate event receives one outcome:

- Accepted
- Rejected
- Duplicate

Rejected events must retain reason codes and enough lineage for diagnosis. They must not enter the trusted Silver event dataset.

## Compatibility

- Adding an optional field may be backward compatible.
- Removing or renaming a field is breaking.
- Changing a field type or meaning is breaking.
- Changing identity semantics is breaking.
- Breaking changes require a new major contract version and migration plan.

## Integrity invariants

- `NASA_ORIGINAL` implies `is_synthetic = false` and no replay or generation ID.
- `NASA_REPLAY` implies `is_synthetic = false` and a non-empty replay run ID.
- `SYNTHETIC_SCALE_TEST` implies `is_synthetic = true` and a non-empty generation ID.
- Every replay must retain an identifiable original lineage root.
- All published counts must distinguish event messages from unique underlying NASA detections.
