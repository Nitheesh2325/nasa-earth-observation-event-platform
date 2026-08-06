# Project State

## Current milestone

Phase 5A - Kafka KRaft dependency research and local streaming architecture complete; implementation approval pending.

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
- Kafka has not been started and no Kafka client dependency has been installed.
- The admitted 100,000-message replay artifact passed the Spark Bronze-to-Silver job in the pinned container.
- Spark accepted 100,000, rejected zero, identified zero duplicates, and read back 100,000 Silver rows.
- The measured run completed in 61.232 seconds at 1,633.13 records per second.
- Silver contains 16 Parquet files totaling 38,399,832 bytes under the preserved `event_date=2026-04-01` partition.
- Independent Silver verification confirmed replay identities, 10,000 underlying detections, classification, parent lineage, sequence and iteration completeness, schedule boundaries, and derived fields.
- Kafka research selected official broker image `apache/kafka:4.3.1`, Python client `confluent-kafka==2.15.0`, and Spark connector `spark-sql-kafka-0-10_2.13:4.0.2`.
- The laptop-safe single-node KRaft topology, resource limits, explicit topics, retention, producer guarantees, offset reconciliation, checkpoint strategy, observability, and security boundary are documented.
- No Kafka image has been pulled, no client installed, no Compose service created, no topic created, and no message published.

## Approved mission

Build a professional batch and streaming data-engineering platform that processes approximately 10 million NASA-derived event messages using clearly distinguished original NASA records, enriched records, replay events, and synthetic scale-test records.

## Current gate

Phase 5A research and architecture are complete. Owner approval is required before adding Docker Compose, pulling Kafka, installing the Python client, starting KRaft, creating topics, or running bounded fixture smoke tests.

## Next proposed milestone

Phase 5B - implement and validate the pinned Kafka-only Docker Compose service, start the bounded KRaft broker, create and describe topics, install the pinned Python client, implement producer and diagnostic consumer foundations, and pass only fixture and 100-message smoke tests. Stop before full replay publication or Spark streaming.

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

## Integrity reminder

The project must never describe all 10 million processed event messages as original NASA observations.
