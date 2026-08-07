# Spark-Kafka Derived Image Gate

## Result

**Passed** on 2026-08-07. The exact Spark 4.0.2 Kafka connector is packaged in a reproducible, digest-pinned local Linux/AMD64 image. The full 100,000-message Structured Streaming gate was not executed.

## Image identity

| Item | Value |
|---|---|
| Image | `nasa-eo-spark-kafka:4.0.2-v1` |
| Digest | `sha256:d92fdb4dc4cc1febc451308ea17880f48b511f65528cc792120a2345b9d6fff3` |
| Local unpacked size | 795,890,344 bytes |
| Base digest | `sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3` |
| Runtime user | `spark` |
| Platform | Linux/AMD64 |
| Definition revision | `3e58939` |

## Dependency construction

The Phase 5D resolver identified eleven artifacts. Seven already existed in the pinned Spark base image with byte-identical checksums: Hadoop client API/runtime, JSR-305, LZ4, Scala parallel collections, SLF4J API, and Snappy. Adding duplicate copies would risk classpath ambiguity, so the derived layer adds only:

| Artifact | SHA-256 |
|---|---|
| Spark SQL Kafka connector 4.0.2 | `faa232c017b0cb4480c14643eb82abf6d0cac4b4d032059bf999b9246d045eb6` |
| Spark Kafka token provider 4.0.2 | `cc5ccc2f8d0d600cb1c356f3de21fb8b21cfaae16e73e350f4fd25a2d727a7ce` |
| Kafka clients 3.9.1 | `7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9` |
| Commons Pool 2.12.0 | `6d3bd18df8410f3e31b031aca582cc109342358a62a2759ebd0c4cdf30d06f8b` |

Docker BuildKit validates each Maven download with `ADD --checksum`. Inspection inside the finished image independently reproduced all four SHA-256 values. No runtime Ivy cache exists in the image.

## Reproducibility finding

The first ordinary build produced an application manifest `d92fdb4d...` wrapped in a provenance-bearing manifest list. Repeating it kept the application manifest, configuration, and layers identical but changed the attestation manifest and therefore the wrapper digest.

The canonical build disables generated provenance for this local artifact. Two consecutive builds then produced identical digest `d92fdb4d...` and identical size 795,890,344 bytes. A later registry workflow must establish its own signing and provenance policy.

## No-resolution runtime proof

The derived runtime resumed logical fixture run `phase5d-fixture-v1` directly by digest. The command contained no `--packages`, `--jars`, `spark.jars.ivy`, or mounted Ivy cache.

Execution ID: `75a438a9-2f5f-49d2-9ffc-6e163f973bed`.

| Check | Result |
|---|---:|
| New Bronze source rows | 0 |
| New accepted source rows | 0 |
| New rejected source rows | 0 |
| Maximum Kafka lag | 0 |
| Bronze read-back | 3 |
| Silver read-back | 1 |
| Rejected read-back | 1 |
| Reconciled duplicate count | 1 |
| Offset reconciliation | Passed |
| Runtime | 22.951 seconds |

The zero-input recovery proves that the embedded connector loads, communicates with Kafka, reads checkpoint metadata, and completes all three query identities without external dependency resolution.

## Full-gate readiness

The approved design uses a fresh producer manifest to exclude old topic history, a new logical streaming run ID and checkpoints, an Available Now trigger, and a 10,000-offset trigger cap. Expected results are 100,000 Bronze, 100,000 accepted Silver, zero rejected, and zero duplicates, followed by a zero-input checkpoint restart.

Execution remains blocked pending owner approval. Rejected-topic routing, dead-letter handling, AWS deployment, PostgreSQL, and dashboard work are outside this gate.
