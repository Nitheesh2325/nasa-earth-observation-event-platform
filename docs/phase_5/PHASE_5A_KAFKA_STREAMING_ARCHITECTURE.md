# Phase 5A - Kafka KRaft and Local Streaming Architecture

## Status

Research and architecture complete. Image pull, client installation, Docker Compose implementation, topic creation, production, consumption, and Spark Structured Streaming execution require owner approval.

## Objective

Add a production-informed but laptop-safe Kafka streaming layer between deterministic replay and Spark processing without weakening lineage, truthfulness, reproducibility, or the completed batch gates.

## Selected Versions

| Component | Selected version | Reason |
|---|---|---|
| Apache Kafka broker | `apache/kafka:4.3.1` | Current official Apache patch release; KRaft-native and security-maintained |
| Python client | `confluent-kafka==2.15.0` | Production-stable client with Python 3.12 Windows wheel and librdkafka performance |
| Apache Spark | 4.0.2 | Existing pinned local and EMR-compatible runtime |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2` | Exact Spark and Scala binary-version alignment |
| Docker Compose | 5.3.1 | Already installed and verified locally |

Do not use floating `latest` tags or unversioned Python dependencies. After pull or build, record immutable image digests. The Spark connector and its transitive JARs must be resolved reproducibly and baked into a derived Spark image before the measured streaming gate; a network-dependent runtime `--packages` resolution is not sufficient for the final evidence run.

## Local Topology

```text
Windows Python 3.12 producer
confluent-kafka 2.15.0
key = lineage_root_id
        |
        | localhost:9092
        v
Apache Kafka 4.3.1
single KRaft combined broker/controller
Docker named data volume
        |
        | kafka:29092 on private Docker network
        v
Spark 4.0.2 + exact Kafka connector
Structured Streaming
        |
        +--> Kafka Bronze landing with offsets and timestamps
        +--> rejected outcome storage
        +--> trusted Silver Parquet
        +--> checkpoint and batch manifests
```

The single combined node exists only for local development. A production deployment must separate controller and broker roles, use at least three controllers, multiple brokers, replication factor three, authentication, encryption, and failure-domain distribution.

## Kafka Container Envelope

| Resource | Local limit |
|---|---:|
| CPUs | 2 |
| Container memory | 1.5 GiB |
| JVM initial heap | 512 MiB |
| JVM maximum heap | 768 MiB |
| Broker/controller nodes | 1 combined node |
| Persistent storage | Docker named volume |
| Host client port | `127.0.0.1:9092` |
| Docker-network port | `kafka:29092` |
| Controller port | Internal only |

A Docker named volume is preferred over a Windows bind mount for Kafka logs because Kafka is write-intensive and the existing D: workspace is a mechanical drive. Volume identity and cleanup commands must be documented. Removing the volume is a destructive operation requiring explicit approval.

Kafka and Spark may run together only within these caps:

- Kafka: 1.5 GiB, 2 CPUs
- Spark: 3 GiB, 4 CPUs
- combined Docker allocation: approximately 4.5 GiB plus engine overhead
- Docker Desktop allocation: approximately 7.65 GiB

PostgreSQL, dashboard, and unrelated containers remain stopped during measured streaming tests.

## KRaft Broker Configuration

Required local properties:

- `process.roles=broker,controller`
- explicit `node.id`
- explicit stable cluster ID
- separate controller, internal Docker, and host listeners
- host listener advertised as `localhost:9092`
- Docker listener advertised as `kafka:29092`
- `auto.create.topics.enable=false`
- `delete.topic.enable=true`
- internal offsets replication factor 1
- transaction-state replication factor 1
- transaction-state minimum ISR 1
- default partition count not relied upon
- plaintext listeners bound only for local development

The Compose health check must verify broker API readiness. Container start alone is not a health signal.

## Topic Plan

| Topic | Partitions | Local RF | Retention | Per-partition byte cap | Segment size | Cleanup |
|---|---:|---:|---:|---:|---:|---|
| `eo.events.replay.v1` | 6 | 1 | 24 hours | 128 MiB | 64 MiB | delete |
| `eo.events.rejected.v1` | 3 | 1 | 7 days | 64 MiB | 32 MiB | delete |
| `eo.events.dlq.v1` | 3 | 1 | 7 days | 64 MiB | 32 MiB | delete |

Additional topic rules:

- create topics explicitly after broker health passes
- `min.insync.replicas=1` locally
- maximum message size 2 MiB, well above the measured event size but still bounded
- no compaction for immutable event streams
- verify partition count, replication factor, and overrides after creation
- record topic descriptions as committed evidence, not broker data

Retention byte limits operate per partition. The replay topic therefore has a theoretical local cap of 768 MiB plus indexes and active segment behavior, which is sufficient for the measured 184 MB artifact while protecting the laptop.

## Message Contract

### Key

UTF-8 `lineage_root_id`.

All ten replay messages for one original NASA detection therefore reach one partition and preserve per-lineage producer order. Kafka does not provide global ordering across six partitions.

### Value

- original deterministic replay JSON bytes
- UTF-8 encoding
- canonical schema version 1.0.0
- no mutation before publication
- no embedded credential
- Kafka topic, partition, offset, and broker timestamp remain downstream metadata rather than pre-populated producer content

### Headers

Use bounded non-secret headers:

- `schema_version=1.0.0`
- `source_type=NASA_REPLAY`
- `replay_contract_version=1`
- `producer_run_id=<physical producer execution UUID>`

Headers do not replace fields inside the governed event value.

## Python Producer Guarantees

Selected dependency: `confluent-kafka==2.15.0` without Schema Registry extras during the first milestone.

Required producer configuration:

- `bootstrap.servers=localhost:9092`
- stable descriptive `client.id`
- `acks=all`
- `enable.idempotence=true`
- bounded retries: 5 application-visible retry attempts where applicable
- `delivery.timeout.ms=120000`
- `request.timeout.ms=30000`
- bounded retry backoff
- `max.in.flight.requests.per.connection<=5`
- `compression.type=zstd`
- `linger.ms=10`
- `batch.size=65536`
- bounded local queue with backpressure through `poll()`
- delivery callback for every message
- final `flush()` with timeout and nonzero undelivered count treated as failure

Producer idempotence prevents duplicates caused by supported in-session retries. It does not make a completely restarted producer execution globally exactly-once. Stable replay `event_id` values and downstream deduplication remain mandatory.

## Producer Modes

### Unpaced benchmark mode

Publish as quickly as bounded producer backpressure allows. Measure actual messages/second, bytes/second, retries, delivery failures, partition distribution, and broker end offsets.

### Scheduled replay mode

Use `scheduled_replay_timestamp` to target 100 messages/second. Record scheduling drift and achieved rate. Never present the logical 10-millisecond schedule as achieved Kafka throughput.

The first full 100,000-message publication should use unpaced mode to establish capacity. Scheduled mode is a later comparison using a fresh topic or explicit run boundary.

## Producer Reconciliation

A successful publication run must prove:

- input artifact count = 100,000
- attempted = delivered + failed
- delivered = 100,000
- failed = 0
- undelivered after flush = 0
- sum of replay-topic partition end offsets added by the run = 100,000
- all six partitions receive records
- producer input checksum equals admitted replay checksum
- no event bytes were modified

The manifest must record client version, librdkafka version, broker version, topic configuration, run ID, start/end offsets, per-partition counts, retries, duration, throughput, bytes, and limitations.

## Consumer and Offset Rules

Python diagnostic consumers and Spark use different explicit group IDs. Diagnostic consumers:

- disable automatic offset commits
- start from a recorded boundary
- commit only after successful validation when commits are part of the test
- record consumed, invalid, duplicate, and partition counts
- stop at recorded end offsets rather than waiting indefinitely

Spark's Kafka source does not use Kafka consumer commits as pipeline truth. Structured Streaming offsets are managed in its checkpoint. A checkpoint belongs to one query identity and must never be reused after incompatible query, schema, or source changes.

## Spark Connector Packaging

Use the exact connector:

`org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2`

For the compatibility smoke test, exact-version package resolution may be used once. Before the measured run:

1. derive from the pinned Spark image digest
2. add the exact connector and transitive dependencies
3. record resolved artifact names and checksums
4. build and pin the derived image digest
5. run without downloading dependencies at job startup

This removes external-network variability from streaming performance evidence.

## Structured Streaming Architecture

### Stage 1: Kafka-to-Bronze landing

Read replay topic values and preserve:

- topic
- partition
- offset
- Kafka timestamp and timestamp type
- key
- headers where supported
- raw value bytes
- landing timestamp
- streaming run ID

Write an append-only partitioned Parquet landing dataset with a dedicated checkpoint. This provides replayable Kafka evidence before trusted transformations.

### Stage 2: validation and Silver admission

Parse values with the explicit version 1 schema and DataFrame expressions. Validate replay identity, classification, parent lineage presence, scheduled timestamp, iteration, sequence, coordinates, measurements, and required fields.

Use `scheduled_replay_timestamp` for streaming event-time watermarks. Do not use the original April NASA observation timestamp as the replay watermark clock.

Deduplicate on `event_id` with a bounded watermark appropriate to the replay schedule. Preserve original `event_timestamp` for scientific partitioning and reporting.

### Multi-output strategy

Avoid non-idempotent ad hoc writes from `foreachBatch`. Each microbatch must use deterministic paths keyed by streaming run ID and Spark batch ID, plus a batch manifest. A retried batch verifies an existing successful manifest rather than appending the same records again.

Accepted, rejected, and duplicate outcomes remain physically distinguishable. The detailed implementation and failure recovery tests belong to the Structured Streaming implementation milestone.

## Initial Streaming Parameters

- trigger: 5 seconds
- `startingOffsets=earliest` only with a fresh dedicated topic boundary and checkpoint
- `failOnDataLoss=true`
- `maxOffsetsPerTrigger=10000`
- Kafka partitions: 6
- Spark shuffle partitions: 16 initially
- watermark: 10 minutes on `scheduled_replay_timestamp`
- checkpoint: unique ignored path containing query name and contract version
- output: unique streaming run root

These are starting values, not tuned claims. Changes require measured evidence.

## Observability

### Broker

- container health and restart count
- Docker CPU and memory
- broker log errors and warnings
- topic partition end offsets
- disk/volume size
- request and network metrics available through JMX

Do not add Prometheus or Grafana during the first gate. Expose JMX only on localhost if required and record selected metrics with a lightweight bounded collector later.

### Producer

- attempted, delivered, failed, retried, and queued messages
- records/second and bytes/second
- delivery latency summary
- per-partition delivery counts
- flush remainder

### Consumer and Spark

- start/end/current offsets by partition
- consumer lag
- input rows and processed rows per second
- batch duration
- state-store rows and memory
- watermark
- accepted, rejected, and duplicate counts
- checkpoint and batch IDs

Spark query progress JSON and compact run manifests are committed as evidence; raw logs remain ignored.

## Security Boundary

Local milestone:

- plaintext only on localhost and the private Docker network
- no public listener
- no credentials in Compose or Git
- no broker UI exposed
- no remote access

Production standard:

- TLS in transit
- SASL or cloud-native authentication
- authorization/ACLs
- separate principals for producer, streaming consumer, and administration
- secrets from an approved secret manager
- network isolation and least privilege
- replication factor three and multiple failure domains

Local plaintext is never described as production-ready security.

## Test Sequence

1. Validate Compose configuration without starting services.
2. Pull image, record digest and size.
3. Start broker alone and pass health check.
4. Create and describe all topics explicitly.
5. Install pinned Python client and verify client/library versions.
6. Publish and consume a three-message accepted/rejected/duplicate fixture.
7. Publish a bounded 100-message replay slice and reconcile keys and offsets.
8. Publish the full 100,000-message artifact in unpaced mode and reconcile offsets.
9. Stop and record producer/broker performance before Spark.
10. Build and pin the Spark-Kafka image.
11. Run Kafka-to-Bronze Structured Streaming with a fresh checkpoint.
12. Run validation-to-Silver streaming and reconcile outputs.
13. Test checkpoint restart without duplicate Silver output.
14. Test rejected and dead-letter paths deliberately.
15. Stop before advancing scale or adding monitoring services.

Each step is a separate gate; a failure stops later steps.

## Risks and Controls

| Risk | Control |
|---|---|
| Combined broker/controller is mistaken for production architecture | Label it local-only and document the separate-role production topology. |
| Kafka and Spark exhaust Docker memory | Enforce 1.5-GiB Kafka and 3-GiB Spark caps; stop unrelated containers. |
| Mechanical disk distorts broker performance | Use a Docker named volume and disclose laptop storage limitations. |
| Producer restart duplicates messages | Enable idempotence, preserve stable event IDs, and deduplicate downstream. |
| Automatic topic creation hides configuration errors | Disable it and verify explicit topic descriptions. |
| Retention consumes the laptop disk | Apply time and per-partition byte limits and monitor volume size. |
| Scheduled timestamps are confused with throughput | Record broker timestamps and actual producer metrics separately. |
| Spark downloads connector JARs during benchmark | Bake and pin a derived Spark-Kafka image first. |
| Checkpoint reuse corrupts semantics | Version checkpoint paths and prohibit reuse after incompatible changes. |
| Multiple output writes duplicate on retry | Use deterministic batch IDs and idempotent batch manifests. |
| Plaintext local listeners are presented as secure | Bind host access to localhost and explicitly separate production security requirements. |

## Phase 5B Boundary

After approval, Phase 5B may:

1. add a pinned Kafka-only Docker Compose definition
2. pull `apache/kafka:4.3.1` and record its digest
3. start the resource-limited single-node KRaft broker
4. create and verify the three explicit topics
5. add and install `confluent-kafka==2.15.0`
6. implement bounded producer and diagnostic consumer contracts and tests
7. run only fixture and 100-message smoke tests
8. record broker, topic, client, offset, and delivery evidence
9. stop before full 100,000-message publication or Spark connector installation

Phase 5B may not publish the full replay artifact, build the Spark-Kafka image, run Structured Streaming, deploy AWS resources, or add monitoring services without separate approval.

## Phase 5A Completion Criteria

- exact broker, client, and connector versions are selected from official sources
- local and production KRaft topologies are distinguished
- laptop CPU, memory, and storage limits are explicit
- topics and retention controls are explicit
- producer delivery and reconciliation guarantees are explicit
- consumer offset and Spark checkpoint responsibilities are explicit
- streaming watermark and idempotent output strategy are defined
- security boundaries and observability requirements are defined
- no dependency, image, Compose service, topic, or message has been added
- owner approval is required before Phase 5B implementation

