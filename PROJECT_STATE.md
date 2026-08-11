# Project State

## Current milestone

Version 1.0 Local release closure: local platform, CI definition, recovery evidence, documentation, and recruiter assets.

## Current status

- The complete local platform is proven end to end at 1,000,000 controlled replay events representing 10,000 original NASA detections exactly 100 times each.
- The 10,000,000-event experiment proves deterministic generation twice and independent read-back for 10,000 original NASA detections replayed exactly 1,000 times.
- Local 10M Spark processing is not proven. The bounded 3 GiB JVM / 5 GiB container attempt reached a verified Java heap ceiling after approximately 629 seconds and produced no admitted output.
- AWS architecture and infrastructure definitions are prepared but not deployed. No AWS resources or workloads exist and actual project cost is $0.00.
- Managed 5M and 10M execution is deferred to Version 1.1. No Version 1.1 work is authorized by this release.
- Version 1.0 Local includes a GitHub Actions workflow for locked dependencies, tests, package/import validation, Docker/Compose validation, repository auditing, secret scanning, data-exclusion validation, and documentation-link validation.
- Kafka and PostgreSQL stop/restart recovery preserved exact offsets, serving truth, governed aggregates, and the admitted load identity. All temporary services were stopped after verification.

- Phase 9B now has separate budget-first and platform-foundation CloudFormation templates, inert parameter/job examples, local validation, authenticated inventory, confirmation-gated deployment/teardown, eight infrastructure contract tests, and explicit live closure criteria.
- The foundation defines the $50 budget, $25/$40 billing alarms, required tags, KMS, private encrypted/versioned S3, private regional networking, least-privilege EMR runtime role, bounded EMR 8.0.0 Spark application, encrypted logs, failure alarm, and dashboard.
- It deliberately defines no scale job, RDS instance, ECS task, MSK/EKS cluster, NAT gateway, load balancer, public endpoint, or application feature.
- Local infrastructure validation passes. AWS service-side validation, live resource verification, notification confirmation, and teardown evidence require explicit authenticated deployment approval and remain pending.
- No AWS resource has been created and actual AWS cost remains $0.00.
- Phase 9A defines the exact `us-east-1` deployment, least-privilege roles, encrypted S3 layout, EMR Serverless capacity, temporary RDS/PostGIS serving gate, Fargate load/verification boundary, CloudWatch controls, $50 budget gate, and ordered teardown.
- The plan excludes managed Kafka, Kubernetes, public API/dashboard hosting, and continuously running compute from Version 1.0 AWS scope.
- The 5M gate is strictly before the 10M gate; each must reconcile identities, underlying detections, replay/synthetic truth, counts, quality, runtime, cost, recovery, and teardown.
- No AWS resources or credentials were created or used. Actual AWS project cost remains $0.00.
- `AGENTS.md` is the active repository instruction file.
- Streamlit 1.61.1 provides one professional dark dashboard that consumes only the six approved FastAPI routes.
- Mission, pipeline truth, daily activity, health/cache/Airflow, bounded geospatial, and detection-lineage views render verified one-million-row API data.
- Geospatial requests remain bounded to seven activity days and 500 points; lineage responses remain bounded to 100 events; daily responses remain bounded to 200 aggregate rows.
- Loading, empty, validation, API-unavailable, large-input, map-filter, lineage-search, and successful-render states are verified.
- The dashboard source contains no psycopg import, SQL, direct database access, local operational metadata access, production mock data, or duplicated aggregate logic.
- Ten live browser reloads measured 1,731 ms median and 2,500 ms p95 to the daily-chart-ready state; the 250-point map completed in 2,079 ms and 100-event lineage in 311 ms.
- Three admitted 1280x720 dark-theme screenshots document overview, map, and lineage behavior; a 900x800 breakpoint had no horizontal overflow.
- Nine dashboard tests and the complete 100-test project discovery pass under the environment-specific boundaries.
- `GET /v1/platform/status` is the single operational endpoint and exposes only bounded dashboard-required metadata.
- A one-row serving view projects the latest successful Gold/load reconciliation without granting the API role access to underlying load-control tables.
- Existing immutable Phase 7 manifests provide latest Airflow state and last-successful execution time through a 1,000-manifest read bound.
- The endpoint exposes cache configuration and entry count but never keys, values, paths, credentials, SQL, secrets, or infrastructure configuration.
- Eighteen focused API tests, five live one-million-row PostgreSQL tests, three isolated Airflow tests, and the complete regression suite pass.
- Thirty local requests measured operational-status latency at 38.396 ms p50 and 52.392 ms p95.
- Only the bounded platform-summary and daily-aggregate responses are cached after request and response validation.
- The in-process cache uses deterministic versioned keys, a 60-second TTL, 256-entry bound, 65,536-byte entry bound, 4,194,304-byte total bound, and LRU eviction.
- `Cache-Control: no-cache` and `no-store` bypass both reads and writes; cache failures fall through to the unchanged read-only PostgreSQL repository.
- Readiness, lineage, bounding-box detail, invalid requests, database failures, and failed response validation are never cached.
- Thirteen API/cache unit tests, four live one-million-row PostgreSQL tests, three isolated Airflow tests, and the complete regression suite pass.
- Thirty-sample cache-hit p95 was 5.139 ms for platform summary and 10.620 ms for daily activity, versus 54.280 ms and 50.898 ms through explicit PostgreSQL bypass.
- FastAPI 0.139.2 and Uvicorn 0.51.0 provide the bounded Version 1.0 read-only API surface.
- Five GET-only routes cover readiness, platform truth summary, activity-time daily aggregates, paginated detection lineage, and bounded GeoJSON bounding-box search.
- Request and response schemas reject unknown parameters, invalid coordinates, inverted boxes, ranges over policy limits, malformed cursors, and excessive limits.
- `eo_api_runtime` is a non-owner, nonsuperuser login inheriting only `eo_api_readonly`; read-only transactions and a 15-second statement timeout are role defaults.
- INSERT, UPDATE, DELETE, and CREATE attempts through the API role all fail with read-only transaction errors.
- A Phase 6 compatibility migration preserves the original observation-date aggregate and adds a separate activity-date aggregate maintained by the existing loader.
- The bounded spatial endpoint uses the existing `event_detail_geometry_gist_idx`; the final exact-predicate plan completed in 0.720 ms.
- Seven API unit tests and four one-million-row PostgreSQL integration tests pass.
- One Airflow 3.3.0 DAG now orchestrates the approved batch vertical slice in the fixed extraction-to-verification order.
- The DAG has bounded parameters, stable logical-date-derived run identities, bounded retries, explicit task and DAG timeouts, `all_success` failure propagation, and one active run.
- Atomic operational manifests record stage attempts, states, durations, checksums, reconciliation, and same-identity rerun reuse outside Git.
- Three Airflow DAG contract tests and sixty-five non-Airflow regression tests pass; the normal suite reports sixty-eight tests with the three isolated Airflow tests skipped.
- A four-record, nine-task `dag.test()` integration execution passed in the official Airflow 3.3.0 Python 3.12 Linux image.
- A same-logical-date rerun passed with the identical orchestration run ID, seven unchanged successful stage receipts, and an explicit idempotent rerun event.
- Native Windows Airflow execution was rejected after its unsupported `os.register_at_fork` boundary was reproduced; the supported Linux-container runtime is the local integration standard.
- Git has been initialized on the `main` branch.
- The initial controlled foundation has been preserved through Git.
- Python 3.12.10 is installed and available.
- Eclipse Temurin OpenJDK 17.0.20 is installed and `JAVA_HOME` is configured.
- Docker Desktop 4.84.0 and Docker Engine 29.6.2 are operational.
- Docker Compose 5.3.1 and the WSL2 Linux backend are available.
- A NASA FIRMS API key has been obtained; the secret remains outside Git.
- A standard-library bounded NASA FIRMS extractor is implemented.
- Eleven extraction tests pass.
- A live 21-record NASA NRT extraction reconciled successfully.
- Deterministic version 1 source identity and canonical event transformation are implemented.
- Twenty automated extraction and canonicalization tests pass.
- All 21 verified NASA rows canonicalized successfully with unique stable identities.
- A repeat canonicalization produced byte-identical canonical event output.
- Local NASA source and generated event data remain excluded from Git.
- No AWS resources have been created; Phase 9A is documentation and planning evidence only.
- Spark dependency research selected PySpark 4.0.2 for parity with the target `emr-spark-8.0.0` runtime.
- The deterministic Standard Processing acquisition and exact 10,000-record selection contract is documented.
- PySpark 4.0.2 and Py4J 0.10.9.9 are installed in the ignored project-local virtual environment.
- Spark 4.0.2 starts with Java 17, but native Windows Parquet output fails because Hadoop requires unavailable Windows filesystem support.
- The official Spark 4.0.2 Python image is pinned to digest `sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3`.
- A resource-limited Linux container wrote and read Parquet with exact two-row reconciliation.
- A fixed 2026-04-01 global `VIIRS_SNPP_SP` request produced 44,292 original NASA records.
- All 44,292 records canonicalized successfully with zero rejections and zero duplicates.
- The deterministic selector produced exactly 10,000 original NASA events and an independent repeat produced an identical checksum.
- Twenty-three automated tests pass.
- The version 1 explicit Spark schema and DataFrame-only validation and deduplication job are implemented.
- A representative integration fixture reconciled one accepted, one rejected, and one duplicate event.
- The measured 10,000-record run accepted 10,000, rejected zero, identified zero duplicates, and read 10,000 Silver rows back from Parquet.
- The job completed in 36.029 seconds at 277.56 records per second on the bounded laptop container.
- Silver contains eight Parquet files totaling 3,913,774 bytes under `event_date=2026-04-01`.
- Twenty-five automated tests pass.
- The version 1 controlled replay contract defines 100,000 replay messages derived from 10,000 unique original NASA detections.
- Deterministic replay run identity, event identity, ordering, scheduling, lineage, and reconciliation rules are documented.
- The future Kafka replay, rejected, and dead-letter topics and stable `lineage_root_id` message key are documented.
- The dependency-free `nasa-replay-v1` identity and streaming JSONL generator are implemented.
- The explicit Spark schema and replay validation rules include scheduled timestamp, replay iteration, and replay sequence fields.
- Thirty-two automated tests pass.
- Two independent generation executions each produced exactly 100,000 `NASA_REPLAY` messages and byte-identical output.
- The replay artifact represents 10,000 unique original NASA detections exactly ten times each and contains zero synthetic messages.
- The admitted replay artifact is 184,078,310 bytes with SHA-256 `9380341108650b2a5b536f9245148abf572883eb7b13ba0c332d0583fb5e0b0a`.
- Independent scanning verified identity uniqueness, lineage resolution, classification, sequence completeness, detection frequency, and scheduled timestamp boundaries.
- The admitted 100,000-message replay artifact passed the Spark Bronze-to-Silver job in the pinned container.
- Spark accepted 100,000, rejected zero, identified zero duplicates, and read back 100,000 Silver rows.
- The measured run completed in 61.232 seconds at 1,633.13 records per second.
- Silver contains 16 Parquet files totaling 38,399,832 bytes under the preserved `event_date=2026-04-01` partition.
- Independent Silver verification confirmed replay identities, 10,000 underlying detections, classification, parent lineage, sequence and iteration completeness, schedule boundaries, and derived fields.
- Kafka research selected official broker image `apache/kafka:4.3.1`, Python client `confluent-kafka==2.15.0`, and Spark connector `spark-sql-kafka-0-10_2.13:4.0.2`.
- The laptop-safe single-node KRaft topology, resource limits, explicit topics, retention, producer guarantees, offset reconciliation, checkpoint strategy, observability, and security boundary are documented.
- The official Kafka 4.3.1 image is pinned by immutable digest and the Python client is pinned at `confluent-kafka==2.15.0`.
- A resource-bounded, single-node local KRaft service passed its health check with an IPv4-only host binding.
- Replay, rejected, and dead-letter topics were explicitly created; live partition, replication, retention, segment, and message-size settings match their contracts.
- Thirty-five automated tests pass.
- A three-message diagnostic fixture reconciled all offsets and correctly identified one duplicate event ID and one missing event ID with zero key/lineage mismatches.
- Exactly 100 checksum-admitted `NASA_REPLAY` messages were acknowledged and reconciled against an offset delta of 100 across all six partitions.
- The bounded consumer read exactly those 100 offsets and found 100 unique event IDs, zero duplicates, zero invalid JSON values, zero missing event IDs, and zero key/lineage mismatches.
- The checksum-admitted 100,000-message `NASA_REPLAY` artifact was fully published with 100,000 acknowledgements, zero delivery failures, and an exact broker offset delta of 100,000.
- The full producer run completed in 11.825 seconds at 8,457.01 records per second and distributed records across all six partitions within 3.4% of the 16,666.67-record mean.
- The diagnostic consumer read exactly the recorded full-run offset ranges and reconciled 100,000 messages and 100,000 unique event IDs with zero duplicates, invalid JSON values, missing event IDs, or key/lineage mismatches.
- Official Spark 4.0.2 documentation reconfirmed connector coordinate `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2` for the Scala 2.13 runtime.
- The exact connector and ten resolved transitive JARs are cached outside Git with recorded SHA-256 checksums and packaged into the pinned derived image.
- Thirty-seven automated tests pass.
- A three-message Kafka fixture passed bounded Structured Streaming with three landed Bronze rows, one accepted Silver row, one rejected quarantine row, and one watermark-bounded duplicate.
- Kafka start and end offsets reconciled exactly, and broker topic, partition, offset, timestamp, key, value, and headers were preserved in Bronze.
- The accepted query uses `scheduled_replay_timestamp` with a ten-minute watermark and `event_id` deduplication.
- A same-checkpoint restart processed zero new rows in all three queries, reported zero lag, and left output counts unchanged.
- Streaming execution manifests are immutable per physical execution after correcting an initially discovered overwrite risk.
- A checksum-enforced Dockerfile adds only the four connector artifacts absent from the pinned Spark base image; seven byte-identical transitive dependencies are reused from the base.
- The derived `nasa-eo-spark-kafka:4.0.2-v1` image is pinned to single-platform digest `sha256:d92fdb4dc4cc1febc451308ea17880f48b511f65528cc792120a2345b9d6fff3` and is 795,890,344 bytes locally.
- Two provenance-disabled builds produced the same derived image digest and size.
- A zero-input checkpoint recovery using the derived runtime succeeded without `--packages`, `--jars`, an Ivy path, or a runtime dependency cache.
- The derived-image proof processed zero new rows in all three queries, reported zero lag, and preserved the fixture's 3/1/1/1 reconciliation.
- The full 100,000-message Structured Streaming gate now has explicit fresh producer boundaries, runtime limits, reconciliation rules, recovery rules, evidence requirements, and exclusions.
- A fresh producer run published exactly 100,000 checksum-admitted `NASA_REPLAY` messages with 100,000 acknowledgements, zero failures, and an exact offset delta of 100,000.
- The full producer completed in 11.287 seconds at 8,859.67 records per second.
- The digest-pinned Spark-Kafka image processed only the fresh producer boundary with no runtime dependency resolution.
- Structured Streaming landed 100,000 Bronze rows, admitted 100,000 unique Silver rows, rejected zero, and identified zero duplicates with exact per-partition offset reconciliation.
- Independent Silver read-back verified 100,000 unique event IDs, 10,000 unique NASA lineage roots, only `NASA_REPLAY`, and zero synthetic rows.
- The first full execution completed in 284.073 seconds; its logical end-to-end rate was 352.02 messages per second across three sequential queries.
- The accepted state store peaked at 100,000 rows and 46,824,872 bytes; zero rows were dropped by the watermark.
- A same-checkpoint recovery consumed zero new rows in all three queries, reported zero lag, preserved all counts and file sizes, and completed the independent verification.
- Streaming output contains 60 Bronze Parquet files (45,790,714 bytes), 161 Silver files (48,805,944 bytes), and 10 empty-outcome rejected files (56,440 bytes).
- Peak observed Spark memory was approximately 1.61 GiB of 3 GiB; peak observed Kafka memory was approximately 453 MiB of 1.5 GiB.
- Kafka and Spark are stopped after the full streaming gate.
- A governed routing envelope preserves source Kafka coordinates, keys, lineage, original values, reason codes, failure category, and actual bounded attempt count.
- Forty automated tests pass.
- A three-message fault fixture reconciled one valid control, one contract rejection, and one dead-letter outcome.
- The rejected topic increased by exactly one offset and independently verified `INVALID_LATITUDE` and `MISSING_EVENT_ID` with the original lineage key and source coordinate.
- The dead-letter topic increased by exactly one offset and independently verified `PROCESSING_RETRIES_EXHAUSTED`, `RuntimeError`, exactly three attempts, the original lineage key, and source coordinate.
- Both routed envelopes passed version, key, unique-source-coordinate, delivery, flush, and offset reconciliation with zero failures.
- Test fault injection requires an explicit CLI flag and is not part of a production service configuration.
- The digest-pinned PostgreSQL 16.4/PostGIS 3.4.3 serving runtime is implemented.
- The governed Gold run reconciled 10,000 accepted Silver rows to 10,000 Parquet and 10,000 load-artifact rows in 22.243 seconds.
- The final database load staged and inserted exactly 10,000 rows in 3.839 seconds; the identical manifest rerun inserted zero rows.
- Independent verification found 10,000 original NASA-derived events, 10,000 unique detections, zero replay events, zero synthetic events, and zero invalid geometries.
- Daily aggregate totals and 10,000 lineage summaries reconcile to detail.
- Least-privilege role checks, a rolled-back content-conflict probe, database checksums, relation sizes, query plans, and warm local latency were recorded.
- Forty-four automated tests pass.
- Kafka, Spark, and PostgreSQL are stopped. The PostgreSQL named volume is preserved.
- The replay-aware serving verifier explicitly distinguishes event messages, original NASA detections, and synthetic rows; 46 automated tests pass.
- Gold reconciled 100,000 accepted replay Silver rows to 100,000 Parquet and 100,000 load-artifact rows in 94.243 seconds.
- PostgreSQL staged and inserted exactly 100,000 replay rows in 63.627 seconds; an identical-manifest rerun inserted zero.
- Database verification found 100,000 unique replay event IDs, 10,000 unique detections, exactly ten events per detection, zero original-message claims, zero synthetic rows, and zero invalid geometries.
- Replay sequence 0-99,999, iteration 1-10, scheduled boundaries, and 100,000 non-null parent IDs passed.
- The database is 408,867,299 bytes; the event-detail relation is 385,294,336 bytes and the physical named volume is 888.2 MB including WAL and engine overhead.
- Full payload JSONB duplication causes material TOAST storage; the next database scale gate requires an approved storage-layout A/B decision.
- Docker Desktop's engine stopped after the successful Gold run; recovery proved exit code zero, no OOM, and complete reconciliation.
- Kafka, Spark, and PostgreSQL are stopped. The 100,000-row PostgreSQL named volume is preserved.
- The full-payload and compact layouts reconciled 100,000 rows with zero differences after excluding only `event_payload`.
- Compact retained 47 materialized fields, 15 constraints, 6 indexes, governed hashes, Gold identity, raw lineage, and PostGIS geometry.
- Total relation storage decreased from 385,294,336 to 178,126,848 bytes, a 53.77% reduction.
- TOAST storage decreased from 207,142,912 bytes to 8,192 bytes, approximately 99.996%.
- After equal vacuum/analyze maintenance, compact p95 was 18.700 ms summary, 9.618 ms spatial, and 3.366 ms lineage; all met the 20% regression bound.
- Compact layout B is selected, but the production loader and canonical serving table still use layout A.
- Forty-seven automated tests pass.
- PostgreSQL, Spark, and Kafka are stopped. The A/B database volume is preserved.
- The selected compact layout is now the production serving projection; `event_payload` remains authoritative only in governed Gold.
- A clean database rebuild directly staged and inserted 100,000 replay messages in 44.225 seconds from the checksum-validated Gold manifest.
- The replay truth gate reconciled 100,000 unique events to 10,000 NASA detections, zero original-message claims, zero synthetic rows, and zero invalid geometries.
- The identical-manifest rerun inserted zero rows, and its no-op path now verifies persisted Gold-run row count before reporting success.
- A deliberately conflicting content hash was detected inside a forced rollback; the serving count remained 100,000.
- The compact event-detail relation is 178,159,616 bytes and the database is 201,642,467 bytes, approximately 53.76% and 50.68% smaller than the full-JSONB gate respectively.
- Forty-nine automated tests pass.
- PostgreSQL, Kafka, and Spark are stopped. The clean compact 100,000-row PostgreSQL evidence volume is preserved.
- Phase 6F defines a sequential local one-million gate using 1,000,000 controlled replay messages derived from 10,000 NASA detections exactly 100 times each.
- The one-million design includes measured storage forecasts, a 40-GB free-disk floor, bounded memory and two-hour subgate limits, immutable failure evidence, and six ordered execution subgates.
- The owner has provided standing approval through Phase 10; scale gates and milestone completion rules still apply and cannot be bypassed.
- Two immutable one-million replay executions produced the same logical run identity, 1,842,603,090 bytes, and SHA-256 `67d32855fe894c6b4e2a5237045f25db4374762320d6a561dc0d438efa2e7778`.
- The first and second one-million generation runs completed in 55.911 and 49.777 seconds.
- Independent full-artifact verification recomputed every identity, lineage mapping, schedule position, preserved source field, classification, byte count, and checksum in 53.154 seconds.
- The artifact contains exactly 1,000,000 `NASA_REPLAY` messages, 10,000 underlying NASA detections, 100 events per detection, and zero synthetic rows.
- Fifty-one automated tests pass.
- PostgreSQL and Kafka remain stopped; no Spark container ran. Both full one-million artifacts remain outside Git.
- Spark batch processed the admitted one-million replay artifact into 1,000,000 accepted Silver rows, zero rejected rows, and zero duplicates in 149.502 seconds.
- Independent Silver verification found 1,000,000 unique event IDs, 10,000 detections, replay iterations 1-100, sequence 0-999,999, replay-only classification, zero synthetic flags, and complete lineage and derived fields.
- Silver contains 32 Parquet data files totaling 214,918,941 bytes; output read-back reconciled exactly.
- Peak observed Spark memory was 3.745 GiB of the 4-GiB limit, so larger local Spark gates are unsafe without a new capacity decision.
- Fifty-three automated tests pass.
- All Spark containers exited successfully; Kafka and PostgreSQL remain stopped.
- Kafka published exactly 1,000,000 checksum-admitted replay messages with one million acknowledgements, zero delivery failures, and an exact one-million broker offset delta in 64.367 seconds.
- The fail-closed diagnostic consumer read only the producer's explicit partition ranges and reconciled one million unique events in 26.066 seconds.
- Kafka truth verification found 10,000 detections exactly 100 times each, complete sequences and iterations, replay-only classification, zero synthetic flags, zero malformed values, zero duplicates, and zero key/lineage mismatches.
- A transient producer-ID coordinator-loading retry was retained as evidence and recovered within the bounded idempotent client policy.
- The Kafka volume uses 1,580,538,375 bytes including prior history, indexes, metadata, compression, and allocated segments.
- Fifty-four automated tests pass.
- Kafka is stopped with its volume and exact Phase 6G.3 offsets preserved; PostgreSQL and Spark are stopped.
- The digest-pinned Spark-Kafka runtime processed only the preserved one-million producer boundary into 1,000,000 Bronze and 1,000,000 Silver rows with zero rejected or duplicate outcomes.
- First streaming execution duration was 1,967.873 seconds; all three queries ended at zero lag and exact producer offsets.
- The accepted state store updated one million rows, peaked at 144,992 retained rows and 58,680,528 bytes, removed 880,000 rows after watermark advancement, and dropped zero records by the watermark.
- Independent streaming-profile verification proved unique events and sequences, detection frequency, replay iterations, schedule, classification, lineage, and broker/status parity.
- A failed verifier attempt exposed and preserved a batch-versus-streaming schema assumption; the corrected verifier now has explicit profiles.
- Same-checkpoint recovery consumed zero new rows in all queries and preserved all counts, files, bytes, offsets, and truth.
- Streaming produced 246 Bronze, 1,312 Silver, and 41 rejected Parquet files; Gold compaction is mandatory.
- Peak observed Spark memory was 3.600 GiB of 4 GiB, prohibiting a larger local streaming claim without a new capacity decision.
- Fifty-four automated tests pass. Kafka, Spark, and PostgreSQL are stopped; Kafka and checkpoint evidence remain preserved.
- Gold contract 1.1 partitions the PostgreSQL JSON load boundary and requires exact per-part bytes, SHA-256, and rows to reconcile before staging while preserving version 1.0 single-part compatibility.
- A first immutable one-million Gold attempt reconciled all rows but was not admitted because its manifest reported requested partitions as if they were observed physical files; the attempt remains preserved.
- The corrected admitted Gold run `80083cae-e529-462f-bf06-f7576bbc1ecc` processed 1,000,000 batch Silver rows into 1,000,000 Gold Parquet and 1,000,000 load-artifact rows in 167.959 seconds.
- The admitted output contains four Gold Parquet files totaling 181,977,884 bytes and four checksum-addressed JSON load files totaling 2,269,335,690 bytes.
- Independent manifest admission hashed all ten artifacts and reconciled exactly 1,000,000 declared load rows; manifest SHA-256 is `43ada13e40f14ffcdbd93d76702ee0d5918be7a666235a902375a860b491ffb9`.
- Independent Spark truth verification found 1,000,000 unique replay events, 10,000 detections exactly 100 times each, complete sequences and iterations, zero synthetic rows, zero null parents, and zero invalid batch-profile rows in 19.951 seconds.
- Highest sampled Spark memory during the admitted build was 1.946 GiB of 4 GiB; the container exited zero without OOM.
- Fifty-nine automated tests pass. Spark, Kafka, and PostgreSQL are stopped; both immutable Gold attempts remain outside Git.
- A clean PostgreSQL 16.4/PostGIS 3.4.3 database was rebuilt from the checksum-admitted one-million Gold 1.1 manifest using only migrations `001` and `003`.
- The load staged and inserted exactly 1,000,000 rows with zero already present in 408.006 seconds; external time including artifact admission was 417.491 seconds.
- The identical manifest rerun inserted zero rows, returned the original load-run identity, and reconciled 1,000,000 persisted rows.
- Serving verification found 1,000,000 unique replay events, 10,000 detections exactly 100 times each, complete sequences and iterations, zero synthetic rows, zero invalid geometries, and exact daily and lineage aggregate totals.
- A forced-rollback content conflict probe detected exactly one mismatch and preserved the one-million-row serving count.
- The compact event-detail relation is 1,756,102,656 bytes; the database is 1,779,675,619 bytes and the physical data directory is 2,896,650,695 bytes including 1,073,741,824 bytes of WAL allocation.
- Warm local p95 was 143.746 ms for detail source summary, 378.860 ms for the broad spatial bounding box, 2.383 ms for lineage, and 1.963 ms for the daily aggregate.
- Highest sampled PostgreSQL memory was 1.804 GiB of 2 GiB, prohibiting a larger local serving claim without a new capacity decision.
- Windows rejected the prior localhost port 55432; the Compose host port is now configurable and the gate used session-selected port 55382 without changing container port 5432.
- Sixty automated tests pass. PostgreSQL is stopped with the completed one-million evidence volume preserved; Kafka and Spark remain stopped.

## Approved mission

Build a professional batch and streaming data-engineering platform that processes approximately 10 million NASA-derived event messages using clearly distinguished original NASA records, enriched records, replay events, and synthetic scale-test records.

## Current gate

Version 1.0 Local release candidate. The one-million full-platform gate and 10-million deterministic generation/read-back experiment are closed with their distinct claims preserved.

## Deferred milestone

Version 1.1: explicitly approved AWS deployment, budget confirmation, authenticated validation, managed 5M gate, managed 10M gate, actual-cost evidence, and teardown evidence.

## Known constraints

- The laptop has 16 GB RAM; Docker currently has approximately 7.6 GB available.
- The project workspace is on a mechanical D: drive, so Spark shuffle and file-heavy operations may be slower.
- The local 10M Spark attempt reached its bounded JVM heap ceiling; another laptop attempt is prohibited without a new capacity decision.
- AWS deployment and managed scale execution require explicit Version 1.1 approval.
- Major dependencies require approval before installation.
- The verified live extraction contains 21 original NASA records and is not a scale-gate result.
- Near-real-time NASA data may later be superseded by standard-processing data.
- The Python canonicalizer is intentionally bounded; distributed scale processing will use Spark DataFrame APIs.
- Native Windows Spark launch scripts do not safely handle the repository's spaced path, and native Hadoop Parquet writes require Windows support that is not bundled with PySpark.
- The local Kafka volume occupied approximately 1.321 GB after topic creation and 103 messages because Kafka segment files are preallocated; this is not payload size.
- The local one-node Kafka service proves contracts and client behavior, not broker high availability, replication, authentication, or failover.

## Integrity reminder

The project must never describe all 10 million processed event messages as original NASA observations.
