# Changelog

## Phase 6F - One-million-record gate design

- Defined the third scale gate as one million controlled replay messages representing 10,000 underlying NASA detections exactly 100 times each.
- Selected sequential local execution based on measured 100,000-record resource evidence and approximately 993 GB of free project-drive capacity.
- Forecast replay, Spark, Kafka, Gold, compact PostgreSQL, checkpoint, WAL, and physical storage requirements.
- Added six ordered subgates, hard resource and runtime stop thresholds, immutable recovery rules, evidence requirements, and the Phase 7-10 program sequence.
- Recorded standing owner approval through Phase 10 without weakening milestone, scale-gate, cost, or reconciliation controls.

## Phase 6E - Compact PostgreSQL/PostGIS production projection

- Promoted the measured compact PostgreSQL layout through a forward migration that removes only redundant `event_payload` JSONB storage.
- Updated the manifest-governed loader to apply ordered migrations and insert 47 explicit serving fields directly from transient staging.
- Rebuilt an empty PostgreSQL/PostGIS database from the 100,000-row governed Gold manifest in 44.225 seconds.
- Reconciled 100,000 replay events, 10,000 NASA detections, zero synthetic rows, aggregates, PostGIS geometry, roles, and conflict rollback.
- Strengthened idempotent no-op behavior to verify persisted Gold-run rows before returning success.
- Added compact direct-load runtime, storage, query-plan, resource, and limitation evidence; the suite now passes forty-nine tests.

## Phase 6D - PostgreSQL/PostGIS 100,000-row storage A/B gate

- Added an isolated compact candidate, controlled A/B plan, structural migration, parity verifier, and atomic local evidence output.
- Passed 47 automated tests.
- Proved 100,000-row equality after removing only duplicated `event_payload`, with zero missing rows or hash/lineage mismatches.
- Verified equal constraint and index counts, replay truth, geometry, read-only grants, idempotent materialization, and conflict detection.
- Reduced total relation storage 53.77% and TOAST approximately 99.996%.
- Corrected an initially unfair pre-vacuum query comparison and encoded equal `VACUUM (ANALYZE)` maintenance into the benchmark.
- Selected compact layout B for direct-loader implementation while keeping the one-million gate blocked.

## Phase 6C - PostgreSQL/PostGIS 100,000-row replay serving gate

- Parameterized serving verification with explicit original, replay, synthetic, and unique-detection expectations.
- Passed 46 automated tests, including the 100,000-message/10,000-detection replay truth contract.
- Reconciled 100,000 Silver replay rows to Gold Parquet and the database bulk-load artifact.
- Loaded and read back exactly 100,000 PostGIS rows; the identical manifest rerun inserted zero rows.
- Verified replay sequence, iteration, parent lineage, spatial integrity, aggregate counts, roles, content-conflict rollback, index plans, latency, storage, and resources.
- Recorded Docker Desktop's post-job engine interruption without misclassifying the successful, exit-zero Gold job.
- Blocked the one-million database gate pending a measured storage-layout A/B decision for JSONB/TOAST and WAL amplification.

## Phase 6B - PostgreSQL/PostGIS 10,000-row serving gate

- Added a digest-pinned, localhost-only PostgreSQL/PostGIS service with bounded CPU, memory, shared buffers, and connections.
- Added least-privilege schemas and roles, spatially constrained event detail, query indexes, aggregate tables, and immutable load-control records.
- Added checksum-governed Gold generation, portable manifests, bulk-copy staging, transactional reconciliation, content-conflict protection, idempotent reruns, and post-load statistics refresh.
- Passed 44 automated tests and the final clean 10,000-row serving gate with exact truth, spatial, aggregate, security, storage, plan, latency, and resource evidence.
- Stopped PostgreSQL after the gate while preserving its named local volume.

## Phase 6A - PostgreSQL/PostGIS serving design

- Defined PostgreSQL as a rebuildable projection of authoritative compacted Gold artifacts.
- Selected PostgreSQL 16 and the RDS-compatible PostGIS 3.4 line, with exact patch and image digest deferred to the implementation-day compatibility check.
- Defined serving, reference, load-control, and quality schemas; event-detail and aggregate grains; spatial and relational indexes; and API query boundaries.
- Defined staged transactional loading, content-hash conflict detection, idempotency, reconciliation, least-privilege roles, monitoring, recovery, and local/AWS resource envelopes.
- Bounded Phase 6B to an exactly 10,000-row local gate with Kafka and Spark stopped.

All meaningful project changes will be recorded here.

## Unreleased

### Added

- Initialized the engineering-controlled repository.
- Added baseline Git safety rules.
- Added local configuration-name documentation without secrets.
- Added the initial project state and truthful project overview.
- Verified the local Python, Java, Docker, Docker Compose, and WSL2 prerequisites.
- Added the secret-free NASA FIRMS configuration variable name.
- Added the version 1 NASA FIRMS source, canonical event, and Bronze data contracts.
- Added the initial governed data dictionary.
- Implemented bounded NASA FIRMS extraction with immutable Bronze output, checksums, manifests, bounded retries, and secret-safe failure behavior.
- Added eleven standard-library extraction tests.
- Reconciled and documented a live 21-record NASA NRT extraction.
- Added the initial engineering decision record.
- Implemented versioned deterministic NASA FIRMS source identities.
- Implemented canonical event transformation with validation, rejection, duplicate handling, checksums, failure manifests, and reconciliation.
- Added canonical-event and identity tests, bringing the standard-library suite to twenty tests.
- Verified 21 unique canonical NASA original events and a byte-identical deterministic rerun.
- Researched and selected PySpark 4.0.2 for local parity with Amazon EMR Serverless `emr-spark-8.0.0`.
- Documented the minimal Spark dependency boundary and conservative laptop resource envelope.
- Defined a fixed-date `VIIRS_SNPP_SP` global acquisition and deterministic exact 10,000-record selection contract.
- Recorded the Spark runtime and first scale-gate engineering decisions.
- Declared and installed pinned PySpark 4.0.2 with Py4J 0.10.9.9 in the ignored project-local environment.
- Verified that Spark 4.0.2 launches with Temurin JDK 17 and Scala 2.13.16.
- Documented the blocked native Windows Parquet smoke test without claiming success.
- Deferred NASA 10,000-record acquisition until an official Linux Spark container passes Parquet reconciliation.
- Pinned the official Apache Spark 4.0.2 Python image by immutable digest.
- Passed a resource-limited Linux-container DataFrame and Parquet write/read reconciliation smoke test.
- Extracted 44,292 fixed-date global `VIIRS_SNPP_SP` original NASA records.
- Reconciled all 44,292 canonical events with zero rejections and zero duplicates.
- Added a governed deterministic scale-gate selector and three automated tests.
- Produced exactly 10,000 original NASA events and verified byte-identical repeat selection.
- Added the explicit version 1 Spark schema and governed Bronze-to-Silver batch job.
- Added DataFrame validation, stable event-key deduplication, rejected and duplicate quarantine outputs, and read-back reconciliation.
- Added a representative accepted/rejected/duplicate Spark integration fixture and two schema tests.
- Passed the measured 10,000-record Spark batch gate with 10,000 Silver rows reconciled.
- Added the Silver contract, first performance report, and Spark gate quality evidence.
- Defined the version 1 controlled NASA replay contract for the 100,000-record gate.
- Defined deterministic replay run and event identities, ordering, scheduled timestamps, and reconciliation invariants.
- Defined replay, rejected, and dead-letter Kafka topic compatibility and the stable lineage-root message key.
- Documented storage estimates, laptop controls, required tests, and the Phase 4B implementation boundary without generating data.
- Implemented dependency-free deterministic replay plan and event identities.
- Implemented streaming replay JSONL generation with immutable physical executions and failure manifests.
- Extended the explicit Spark schema and validation contract with replay schedule, iteration, and sequence fields.
- Added replay identity, lineage, repeatability, failure, and schema tests, bringing the suite to thirty-two tests.
- Recorded and corrected a Windows path-length failure without changing logical replay identities.
- Generated two byte-identical 100,000-message replay artifacts and independently reconciled their lineage, identities, classifications, sequence, size, and checksum.
- Passed the 100,000-message Spark Bronze-to-Silver gate with complete outcome and Parquet read-back reconciliation.
- Independently verified Silver replay identities, detection counts, classifications, lineage, sequence, schedule, partition, and derived fields.
- Recorded 100,000-gate runtime, throughput, Parquet size, configuration, comparison, and limitations.
- Researched and selected exact Kafka 4.3.1, confluent-kafka 2.15.0, and Spark Kafka connector 4.0.2 versions.
- Defined the laptop-safe single-node KRaft topology and separate production topology.
- Defined explicit topic, retention, resource, producer, consumer, checkpoint, watermark, idempotency, security, and observability standards.
- Defined the gated Kafka and Structured Streaming test sequence without pulling images, installing dependencies, starting services, or publishing messages.
- Pinned the official Kafka 4.3.1 image by immutable digest and installed the approved `confluent-kafka==2.15.0` client.
- Added a resource-bounded single-node KRaft Compose service with an IPv4-only host listener and persistent local volume.
- Implemented explicit topic administration, delivery-accounted bounded publication, offset manifests, and exact-range diagnostic consumption.
- Added Kafka contract tests and a three-message replay diagnostic fixture, bringing the suite to thirty-five tests.
- Verified all three live topic configurations and reconciled both a three-message diagnostic fixture and exactly 100 admitted replay messages.
- Recorded the bounded Kafka smoke evidence and stopped before full replay publication or Spark streaming.
- Published the checksum-admitted 100,000-message replay artifact with idempotent delivery, acknowledgements, bounded retries, and exact per-partition offset reconciliation.
- Consumed only the recorded producer offset ranges and reconciled all 100,000 unique event IDs with zero malformed values, missing IDs, duplicates, or key/lineage mismatches.
- Recorded full Kafka producer, consumer, partition-distribution, and broker-resource evidence; stopped before Spark Structured Streaming.
- Reconfirmed the exact Spark 4.0.2 Scala 2.13 Kafka connector and resolved its eleven-artifact dependency set into the ignored local cache.
- Implemented explicit-offset Kafka Bronze landing, canonical parsing, validation, ten-minute event-time watermarking, `event_id` deduplication, Silver admission, rejected Parquet quarantine, and independent checkpoints.
- Added a governed three-message Structured Streaming fixture and two contract tests, bringing the suite to thirty-seven tests.
- Reconciled the fixture as three landed, one accepted, one rejected, and one duplicate with exact Kafka offset boundaries.
- Verified same-checkpoint recovery with zero new input, zero lag, and unchanged output counts.
- Corrected execution-manifest overwrite risk by assigning immutable physical execution IDs and paths.
- Added a checksum-enforced derived Spark-Kafka Dockerfile and a complete artifact lock.
- Verified seven transitive artifacts already in the pinned Spark base image and added only the four absent JARs.
- Built the single-platform runtime twice with identical digest `sha256:d92fdb4dc4cc1febc451308ea17880f48b511f65528cc792120a2345b9d6fff3`.
- Proved the derived image resumes all three checkpoints with zero new input and no runtime package or Ivy resolution.
- Defined the fresh offset boundary, runtime envelope, reconciliation, recovery, evidence, and exclusion rules for the full 100,000-message streaming gate.
- Republished exactly 100,000 admitted replay messages into a fresh, checksum-verified Kafka offset boundary.
- Passed the full digest-pinned Structured Streaming gate with 100,000 Bronze, 100,000 Silver, zero rejected, and zero duplicate outcomes.
- Verified 100,000 unique event IDs, 10,000 NASA lineage roots, replay-only classification, zero synthetic rows, and exact per-partition offsets.
- Recorded per-query microbatch, lag, state-store, Parquet footprint, runtime, throughput, and Docker resource evidence.
- Passed same-checkpoint recovery with zero new input, zero lag, unchanged outputs, and independent Silver read-back verification.
- Added a versioned failure-routing envelope with preserved Kafka coordinates, lineage, reason codes, attempts, and original values.
- Implemented bounded source consumption, contract rejection, exhausted-retry DLQ routing, delivery callbacks, output watermark reconciliation, and independent envelope verification.
- Added a three-message valid/rejected/exhausted fixture and three tests, bringing the suite to forty tests.
- Reconciled one pass-through control, one rejected-topic message, and one dead-letter message with exact source and destination offsets.
- Independently verified stable lineage keys, rejection reasons, failure category, and exactly three DLQ processing attempts.
