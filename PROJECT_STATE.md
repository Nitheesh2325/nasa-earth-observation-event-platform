# Project State

## Current milestone

Phase 5D - bounded Spark Structured Streaming compatibility and checkpoint recovery complete; full streaming execution is not approved.

## Current status

- `AGENTS.md` is the active repository instruction file.
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
- No AWS resources have been created.
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
- The exact connector and ten resolved transitive JARs are cached outside Git with recorded SHA-256 checksums; they are not yet baked into a pinned derived image.
- Thirty-seven automated tests pass.
- A three-message Kafka fixture passed bounded Structured Streaming with three landed Bronze rows, one accepted Silver row, one rejected quarantine row, and one watermark-bounded duplicate.
- Kafka start and end offsets reconciled exactly, and broker topic, partition, offset, timestamp, key, value, and headers were preserved in Bronze.
- The accepted query uses `scheduled_replay_timestamp` with a ten-minute watermark and `event_id` deduplication.
- A same-checkpoint restart processed zero new rows in all three queries, reported zero lag, and left output counts unchanged.
- Streaming execution manifests are immutable per physical execution after correcting an initially discovered overwrite risk.
- No full 100,000-message Structured Streaming run, rejected-topic publication, dead-letter flow, or derived Spark-Kafka image build has occurred.

## Approved mission

Build a professional batch and streaming data-engineering platform that processes approximately 10 million NASA-derived event messages using clearly distinguished original NASA records, enriched records, replay events, and synthetic scale-test records.

## Current gate

Phase 5D is complete. Owner approval is required before packaging the resolved connector JARs into a reproducible derived Spark image and designing the full 100,000-message streaming gate.

## Next proposed milestone

Phase 5E - define and build the derived Spark-Kafka image from recorded artifacts, pin its digest, design fresh source boundaries and idempotent multi-output evidence, and stop for approval before the full 100,000-message Structured Streaming execution.

## Known constraints

- The laptop has 16 GB RAM; Docker currently has approximately 7.6 GB available.
- The project workspace is on a mechanical D: drive, so Spark shuffle and file-heavy operations may be slower.
- The 5-million and 10-million execution environments will be chosen from measured earlier-gate results.
- AWS deployment is prohibited until the local 10,000-record vertical slice passes.
- Major dependencies require approval before installation.
- The verified live extraction contains 21 original NASA records and is not a scale-gate result.
- Near-real-time NASA data may later be superseded by standard-processing data.
- The Python canonicalizer is intentionally bounded; distributed scale processing will use Spark DataFrame APIs.
- Native Windows Spark launch scripts do not safely handle the repository's spaced path, and native Hadoop Parquet writes require Windows support that is not bundled with PySpark.
- The local Kafka volume occupied approximately 1.321 GB after topic creation and 103 messages because Kafka segment files are preallocated; this is not payload size.
- The local one-node Kafka service proves contracts and client behavior, not broker high availability, replication, authentication, or failover.

## Integrity reminder

The project must never describe all 10 million processed event messages as original NASA observations.
