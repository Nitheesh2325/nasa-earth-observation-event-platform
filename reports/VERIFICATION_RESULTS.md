# Verification Results

## Verification Scope

Version 1.0 Local proves a complete local pipeline at 1,000,000 events. The separate 10,000,000-event experiment proves deterministic replay generation and independent read-back only; Spark did not complete at that scale because the safe local JVM heap ceiling was reached. AWS infrastructure is defined and locally validated but was not deployed. Generated scale data remains outside Git.

## Source Data

- Official source: NASA FIRMS `VIIRS_SNPP_SP`.
- Canonical extraction admitted 44,292 records; a deterministic 10,000-record subset was selected for controlled replay.
- Raw extraction: 3,466,282 bytes, SHA-256 `40386d30b04efd4ff32e001d59f8b3fccda20b6e2d878b9f598322bb29a9734e`.
- Selected canonical data: 15,227,942 bytes, SHA-256 `3f711f63138ae1e5c926d5dcd6edf3a15094ae8f7d8631db4e566b936862ede3`.
- Repeated selection produced the same checksum. Every event retains source dataset, source record identity, timestamps, run identity, and explicit original/replay/synthetic classification.

## Deterministic Replay

- 1,000,000 events were generated from 10,000 NASA detections replayed exactly 100 times.
- Output: 1,842,603,090 bytes; SHA-256 `67d32855fe894c6b4e2a5237045f25db4374762320d6a561dc0d438efa2e7778`.
- Two generation runs completed in 55.911 s and 49.777 s (17,885.72 and 20,089.41 events/s) with identical output checksums.
- Independent verification read 1,000,000 unique event IDs, 10,000 underlying detections, and no sequence gaps. Classification reconciled to `NASA_REPLAY=1,000,000`, original `0`, synthetic `0`; verification throughput was 18,813.13 events/s.

## Kafka

- 1,000,000 replay events were published to a six-partition KRaft topic with stable event keys, replication factor 1, `acks=all`, and bounded retries.
- Broker offset delta: exactly 1,000,000. Publish runtime: 64.367 s; throughput: 15,536.03 events/s.
- Independent consumption read exactly 1,000,000 events in 26.066 s (38,364.39 events/s), with 1,000,000 unique event IDs, zero duplicates, zero invalid records, and preserved keys and lineage.
- A bounded failure-routing fixture attempted and acknowledged three records, produced an offset delta of three, and had SHA-256 `03271749ed81aaacee06e7e5119e59e57208e4d4c49a7256524b6a4297eb3e80`. Rejection/dead-letter routing and bounded attempts were verified.

## Spark Batch

- Input reconciled to 1,000,000 events, 1,842,603,090 bytes, and SHA-256 `67d32855fe894c6b4e2a5237045f25db4374762320d6a561dc0d438efa2e7778`.
- Result: accepted `1,000,000`, rejected `0`, duplicates `0`, unique event IDs `1,000,000`, underlying detections `10,000`.
- Runtime: 149.502 s; throughput: 6,688.88 events/s with 4 CPUs, a 4 GiB container limit, 3 GiB driver memory, and 32 shuffle partitions.
- Output: 32 Parquet data files totaling 214,918,941 bytes; the complete output directory contained 66 files and 216,598,329 bytes. Independent verification throughput was 36,706.53 events/s. Peak Spark-container memory was 3.745 GiB of 4 GiB.

## Spark Structured Streaming

- Exactly 1,000,000 Kafka events were processed with maximum trigger offsets of 25,000 and 32 shuffle partitions.
- Result: accepted `1,000,000`, rejected `0`, duplicates `0`; total logical workload runtime 1,967.873 s and throughput 508.16 events/s.
- Maximum consumer lag was 167,408. State reached 144,992 rows / 58,680,528 bytes and removed 880,000 rows through watermark progress.
- Query runtimes were 170.536 s for Bronze (5,863.85 events/s), 932.684 s for accepted output (1,072.17 events/s), and 710.921 s for rejected output (1,406.63 events/s). Independent verification throughput was 3,915.66 events/s.
- A checkpoint restart consumed zero new events and left accepted, rejected, and duplicate counts unchanged. Silver contained 1,312 files averaging 369 KiB; the Silver directory was 909,809,939 bytes and checkpoints were 161,079,529 bytes. Peak Spark memory was 3.600 GiB of 4 GiB; Kafka used 514.9 MiB of a 1.5 GiB limit.

## Gold Data

- Gold generation admitted exactly 1,000,000 event-detail rows and reconciled daily and lineage aggregates to the same governed input.
- Runtime: 167.959 s; throughput: 5,953.83 events/s.
- Manifest SHA-256: `43ada13e40f14ffcdbd93d76702ee0d5918be7a666235a902375a860b491ffb9`.
- Four Parquet files totaled 181,977,884 bytes; four governed PostgreSQL JSONL files totaled 2,269,335,690 bytes.
- Independent read-back completed in 19.951 s (50,121.99 events/s). Peak Spark-container memory was 1.946 GiB of 4 GiB.

## PostgreSQL/PostGIS

- PostgreSQL 16.4 / PostGIS 3.4.3 rebuilt from the admitted Gold manifest.
- Staged, inserted, and final serving counts each reconciled to `1,000,000`; unique event IDs were `1,000,000` and underlying NASA detections were `10,000`.
- Geometry validation, source/replay/synthetic classification, replay reconciliation, and aggregate reconciliation produced zero invalid or unexplained rows.
- In-process load runtime was 408.006 s (2,450.94 rows/s); external timing was 417.491 s (2,395.26 rows/s). An idempotent external rerun completed in 48.981 s, inserted zero rows, and identified all 1,000,000 rows as already present. A deliberate identity conflict rolled back without partial mutation.
- Peak database-container memory was 1.804 GiB of 2 GiB. Database size was 1,779,675,619 bytes; detail relation size was 1,756,102,656 bytes; physical volume usage was 2,896,650,695 bytes including 1,073,741,824 bytes of WAL.
- Warm local p95 latency: source summary 143.746 ms, bounding box 378.860 ms, lineage 2.383 ms, and daily aggregate 1.963 ms. GiST-backed geospatial plans were verified.

## Airflow

- One production-style DAG executed the nine-task batch vertical slice in the official `apache/airflow:3.3.0-python3.12` image.
- The four-record integration profile verified explicit ordering, bounded retries and timeouts, failure propagation, stable run identity, operational receipts, and safe rerun behavior.
- Reusing the same logical date produced the same run identifier; seven governed receipts remained unchanged on rerun. This orchestration profile validates behavior and does not replace the 1M scale measurement.

## FastAPI and Cache

- Read-only endpoints cover readiness, operational status, platform summary, daily aggregates, bounded PostGIS bounding-box events, and detection lineage. Validation enforces bounded result limits, coordinates, pagination, parameterized SQL, and a database role with no write capability.
- The verified bounding-box query plan used the PostGIS GiST index and executed in 0.720 ms.
- The operational-status endpoint measured p50 38.396 ms, p95 52.392 ms, and p99 54.174 ms.
- The replaceable bounded cache preserves endpoint contracts and falls back to PostgreSQL on backend failure. Summary cache-hit p95 was 5.139 ms versus 54.280 ms with bypass; daily cache-hit p95 was 10.620 ms versus 50.898 ms with bypass. Hit, miss, expiration, bypass, deterministic keys, entry bounds, backend failure, and fallback were verified.

## Dashboard

- The dark responsive Streamlit dashboard consumes only the six bounded FastAPI endpoints; it has no database access, hidden SQL, mock production data, or duplicated analytics logic.
- Browser tests covered successful, loading, empty, error, unavailable-API, large-dataset, lineage-search, and map-filter states.
- Browser render timing measured median 1,731 ms and p95 2,500 ms; map interaction was 2,079 ms and lineage interaction was 311 ms.

## Recovery and Idempotency

- Kafka stopped and restarted cleanly in 21.183 s and 8.798 s; committed offsets remained exactly 1,200,109.
- PostgreSQL stopped and restarted in 7.321 s and 6.760 s; serving counts and verified truth remained unchanged.
- Governed boundaries were rerun using stable identities. Checkpoint restart consumed no additional Kafka records, PostgreSQL reload inserted no rows, and conflict handling preserved transactional state.

## 10M Local Capacity Boundary

- Exactly 10,000 original NASA detections were replayed 1,000 times to create 10,000,000 controlled replay events. These are not 10,000,000 original observations.
- Two generation runs completed in 490.562 s and 461.594 s (20,384.79 and 21,664.05 events/s). Each output was 18,445,760,890 bytes with SHA-256 `50096a05162ead1c72b879997d23bc16dc382905e8eea937b7e1ee30548952cc`.
- Independent read-back completed in 1,819.625 s (5,495.64 events/s) and reconciled 10,000,000 parsed and unique event IDs, 10,000 underlying detections each appearing 1,000 times, complete replay iterations, `NASA_REPLAY=10,000,000`, original `0`, and synthetic `0`.
- Spark was attempted with 4 CPUs, a 5 GiB container limit, 3 GiB JVM heap, and 128 partitions. It failed after approximately 629 s with a verified Java heap exhaustion boundary. No Spark output or admitted manifest was produced. Therefore 10M Spark, Kafka, streaming, PostgreSQL, Airflow, API, and dashboard execution are not claimed.

## AWS Infrastructure Validation

- CloudFormation defines 32 bounded resources and passed eight infrastructure tests plus local template validation.
- The EMR Serverless application definition caps capacity at 16 vCPU, 64 GiB memory, and 200 GiB disk, starts with zero preinitialized capacity, auto-stops after 10 idle minutes, and sends logs to 30-day CloudWatch log groups.
- Private encrypted S3 Bronze/Silver/Gold boundaries, least-privilege runtime roles, budget definitions, billing alarms, tagging, and teardown inventory are encoded and locally validated.
- AWS resources created: `0`; AWS workloads executed: `0`; actual AWS cost: `$0.00`. The `$50` budget, `$25`/`$40` billing alarms, and `$33-$45` two-gate envelope are future planning controls, not incurred spend. Managed execution and teardown evidence remain future work.

## Test Summary

- Full portable suite: 108 tests collected, 100 passed, 8 skipped, 0 failed.
- Representative integration-safe suite: 19 passed.
- Repository audit, secret scan, generated-data exclusion validation, documentation-link validation, README image validation, dependency consistency checks, Docker Compose validation, and Git whitespace validation passed for the Version 1.0 Local release.
