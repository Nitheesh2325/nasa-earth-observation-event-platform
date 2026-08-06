# Changelog

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
