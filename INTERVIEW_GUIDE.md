# Interview Guide

## Thirty-second summary

ASTRAYAN is a production-style NASA Earth Observation data platform that I built with Python, Kafka, Spark, Parquet, PostgreSQL/PostGIS, Airflow, FastAPI, and Streamlit. I proved the complete local system at one million controlled replay messages representing 10,000 real NASA detections, including deterministic lineage, streaming recovery, compact Gold, read-only geospatial serving, orchestration, caching, and a dashboard. I separately generated and independently verified ten million deterministic replay events, then documented the honest JVM heap boundary when local Spark could not process them. AWS infrastructure is prepared but deliberately deferred with zero cloud spend.

## Quantified resume bullets

- Built a production-style Earth Observation data platform processing **1,000,000 governed event messages end to end**, from deterministic NASA replay through Kafka, Spark batch/streaming, Parquet, PostgreSQL/PostGIS, FastAPI, and Streamlit.
- Designed stable event and lineage identities that reconciled **1,000,000 unique events to 10,000 NASA detections**, with exact replay sequence/iteration checks and zero false synthetic or original-message claims.
- Implemented Spark Structured Streaming with explicit Kafka offsets, watermarks, separate checkpoints, dead-letter/rejection routing, and **zero-input recovery with unchanged counts and zero lag**.
- Reduced PostgreSQL event-detail storage by **53.77%** by replacing duplicated JSONB payload storage with a governed **47-column compact projection**, while preserving constraints, indexes, lineage, and query behavior.
- Built a read-only geospatial API and recruiter dashboard with bounded queries; measured API cache-hit p95 of **5.139 ms** for summary and **10.620 ms** for daily activity.
- Generated and independently verified **10,000,000 deterministic replay events** at up to **21,664 events/s**, documenting the local Spark JVM heap ceiling rather than overstating scale.
- Defined budget-first, least-privilege AWS CloudFormation for S3, EMR Serverless, CloudWatch, KMS, and private networking while maintaining **$0.00 actual AWS cost** in Version 1.0.

## Core interview stories

### Data truth versus scale

The system starts from 10,000 distinct NASA detections. Scale comes from controlled replay, not fabricated claims about original observations. `event_id` identifies a message, while `detection_id` and `lineage_root_id` identify its source detection. That distinction lets every aggregate report messages, unique events, and unique detections separately.

### Idempotency and conflict handling

Logical run IDs derive from immutable inputs and parameters; physical execution IDs preserve separate attempts. Gold manifests carry checksums and row counts. PostgreSQL uses the manifest SHA-256 as an idempotency key, stages bulk data, detects same-ID/different-content conflicts, and promotes transactionally. An identical million-row reload inserted zero rows; an intentional content conflict rolled back without changing serving truth.

### Batch versus streaming

Both use explicit schemas and DataFrame validation. Batch reads governed JSON and writes partitioned Silver. Streaming additionally preserves Kafka topic/partition/offset/timestamp, uses explicit start/end offsets, event-time watermarks, and independent checkpoints for accepted, rejected, and duplicate outcomes. A recovered checkpoint consumed zero new rows and preserved output exactly.

### Storage optimization

The first serving design duplicated full event JSONB and created material TOAST overhead. A controlled A/B benchmark proved that a compact 47-column projection preserved every governed field needed for serving while reducing total event-detail storage 53.77% and TOAST approximately 99.996%. Gold Parquet remains authoritative for complete governed content.

### Honest failure handling

The ten-million generator and independent verifier passed, but local Spark exhausted its 3-GiB JVM heap while materializing cached classified data. The job failed before writes, created no success manifest, and left zero partial Spark files. I stopped instead of raising memory beyond the laptop-safe envelope. This separates proven generation scale from unproven processing scale.

### Cost and cloud discipline

AWS was not needed to prove Version 1.0 locally. The repository includes two-stage CloudFormation that requires a $50 budget and confirmed alarms before the private foundation can deploy. EMR capacity and auto-stop are bounded. Live 5M/10M managed gates, RDS recovery, and actual costs are explicitly Version 1.1; Version 1.0 incurred $0.00.

## Likely questions

**Why Kafka if the source is batch?** Controlled replay converts admitted batch observations into a deterministic stream so offset, lag, checkpoint, watermark, rejection, and recovery behavior can be proved without misrepresenting NASA source frequency.

**Why PostgreSQL when Gold Parquet exists?** Gold is the analytical authority. PostgreSQL/PostGIS is a rebuildable low-latency serving projection for bounded API summaries, lineage, and spatial queries.

**Why not partition PostgreSQL at 10M?** The measured compact unpartitioned design is sufficient for the verified one-million local serving boundary and preserves simpler indexes and constraints. Partitioning should follow measured 10M RDS behavior, not be added speculatively.

**How is late data handled?** Streaming uses scheduled replay event time and a ten-minute watermark. Late/duplicate behavior is explicit and separated from invalid-record rejection.

**What would you do next?** Execute the already-defined Version 1.1 AWS gates in order: budget/identity preflight, managed compatibility smoke, 5M EMR Serverless gate, temporary RDS/PostGIS recovery proof, teardown/cost reconciliation, then 10M only after 5M closes.

## Claims to avoid

- Do not call all replay messages original NASA observations.
- Do not claim local 10M Spark processing, 10M serving, distributed-cluster performance, multi-user load, or cloud deployment.
- Do not claim AWS production readiness from unexecuted CloudFormation.
- Do not describe local single-node Kafka as highly available.
