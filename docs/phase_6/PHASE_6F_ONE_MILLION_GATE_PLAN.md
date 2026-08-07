# Phase 6F - One-Million-Record Gate Plan

## Status

Approved and complete as an execution design. Phase 6G may execute this plan under the owner's standing approval through Phase 10, but every subgate remains fail-closed and evidence-gated.

## Objective

Prove that the existing governed pipeline can process exactly 1,000,000 controlled NASA replay event messages on the local laptop without weakening truth classification, lineage, deterministic identity, bounded resource use, recovery, or count reconciliation.

This is the third required scale gate. It does not claim one million original NASA observations.

## Truthful data composition

| Classification | Event messages | Underlying detections | Repetitions per detection |
|---|---:|---:|---:|
| `NASA_ORIGINAL` | 0 | 0 | 0 |
| `NASA_REPLAY` | 1,000,000 | 10,000 | 100 |
| `SYNTHETIC_SCALE_TEST` | 0 | 0 | 0 |

The input is deterministically derived from the admitted 10,000-record NASA-original scale-gate artifact. Every replay event must retain `detection_id`, `lineage_root_id`, `parent_event_id`, source dataset, ingestion run, raw object lineage, and `is_synthetic=false`. Replay sequence must cover 0 through 999,999 exactly once; replay iteration must cover 1 through 100 exactly 10,000 times each.

## Measured baseline and forecast

Forecasts are planning limits, not results. Linear projections are deliberately widened for state, WAL, shuffle, filesystem, and mechanical-drive variability.

| Component | Measured at 100,000 | One-million planning range |
|---|---:|---:|
| Replay JSONL | 184,078,310 B | 1.80-1.90 GB |
| Batch Silver Parquet | 38,399,832 B | 0.38-0.60 GB |
| Kafka producer time | 11.287 s | 2-5 min |
| Structured Streaming time | 284.073 s | 25-60 min |
| Streaming Bronze + Silver | 94,596,658 B | 0.9-1.5 GB |
| Accepted state store | 46,824,872 B | 0.45-0.80 GB |
| Gold Parquet | 30,660,288 B | 0.30-0.50 GB |
| Gold load artifact | 226,751,570 B | 2.2-2.5 GB |
| Compact event-detail relation | 178,159,616 B | 1.7-2.1 GB |
| Compact database | 201,642,467 B | 1.9-2.5 GB |
| PostgreSQL physical directory | 513,425,863 B | 4.5-7.0 GB |

The D: volume has 993,002,270,720 bytes free at plan time. Local governed data currently uses approximately 1.10 GB. Disk capacity is sufficient, but each subgate must still check for at least 40 GB free before starting to cover Kafka, Spark, Gold, database, WAL, checkpoints, retry evidence, and Docker overhead.

## Execution topology decision

Execute the one-million gate locally and sequentially. The measured 100,000-record workloads stayed inside the 7.647-GiB Docker allocation, and the compact database removed the principal PostgreSQL storage multiplier. Local execution provides stronger scale evidence without AWS cost or premature cloud complexity.

Do not run Spark, Kafka, and PostgreSQL concurrently except where Kafka and Spark are intrinsically required for the streaming subgate. PostgreSQL remains stopped during generation and streaming. Kafka remains stopped during batch, Gold, and database work.

The 5-million and 10-million gates are not authorized for this laptop by this decision; their execution environment will be selected from the one-million measurements.

## Ordered subgates

### 6G.1 - Deterministic replay generation

- Generate exactly 1,000,000 replay messages from the admitted 10,000-event source.
- Write to a new immutable run path; never overwrite the 100,000 artifact.
- Perform a second deterministic execution only if enough temporary disk remains.
- Reconcile output count, event identities, lineage, iterations, sequence, timestamps, classification, bytes, and SHA-256.

### 6G.2 - Spark batch Bronze-to-Silver

- Use the digest-pinned Spark 4.0.2 image, explicit schema, DataFrame APIs, four CPUs, and a maximum 4-GiB container limit.
- Start with 32 shuffle partitions and record file count and size; do not tune after seeing a failure and present the retry as the first run.
- Require `accepted + rejected + duplicate = 1,000,000` and an independent Parquet read-back.

### 6G.3 - Kafka publication and diagnostic consumption

- Use the existing digest-pinned Kafka 4.3.1 KRaft service and six explicit replay partitions.
- Record fresh per-partition start offsets before publishing.
- Require one million acknowledgements, zero delivery failures, exact offset delta, all partitions used, stable lineage keys, and independent bounded consumption.
- Stop if any partition exceeds twice the mean while another is below half the mean.

### 6G.4 - Spark Structured Streaming

- Use the digest-pinned derived Spark-Kafka runtime without dependency resolution.
- Read only the recorded one-million producer boundary.
- Begin with `maxOffsetsPerTrigger=25000`, 32 shuffle partitions, four CPUs, 4-GiB container memory, and a 3-GiB driver ceiling.
- Preserve independent Bronze, accepted, and rejected checkpoints and physical execution manifests.
- Require exact offsets, outcome reconciliation, zero lag at completion, independent Silver read-back, and a same-checkpoint zero-input restart.

### 6G.5 - Governed Gold build

- Read the admitted one-million Silver output through explicit schemas.
- Write compacted partitioned Gold Parquet plus the database load artifact and immutable manifest.
- Reconcile all artifacts by count, bytes, SHA-256, source truth, and Gold run identity.

### 6G.6 - Compact PostgreSQL/PostGIS rebuild

- Remove only the verified disposable Compose database volume after preserving prior compact evidence in Git.
- Build an empty database with migrations `001` and `003`; never apply the A/B projection migration.
- Load directly from the governed one-million Gold manifest.
- Require counts, unique identities, 10,000 detections, 100 events per detection, geometry, aggregates, roles, conflict rollback, persisted-row idempotency, query plans, storage, WAL, runtime, and resource evidence.
- Preserve the completed volume until the gate evidence is committed.

## Resource and failure budgets

| Control | Stop threshold |
|---|---:|
| Host free disk before any subgate | less than 40 GB |
| Spark container memory | greater than 4 GiB or OOM |
| Kafka container memory | greater than 1.5 GiB or OOM |
| PostgreSQL container memory | greater than 2 GiB or OOM |
| Any subgate wall time | greater than 120 minutes |
| Spark state-store memory | greater than 1.0 GiB |
| Unreconciled rows or offsets | greater than 0 |
| Delivery failures | greater than 0 |
| Invalid truth classification | greater than 0 |
| Checksum mismatch | greater than 0 |

A threshold breach is a failed or blocked subgate, not permission to delete evidence, silently increase resources, repartition outputs, or change contracts. Record the failure and design a bounded retry separately.

## Recovery rules

- Every execution gets a new immutable physical run ID and path.
- Never overwrite or edit a successful or failed manifest.
- Never delete a checkpoint to make a restart appear successful.
- Resume only when query identity, source boundary, schema, checkpoint, and sink remain compatible.
- A changed configuration or contract creates a new execution and explicitly references the failed predecessor.
- Gold remains authoritative for database recovery; PostgreSQL remains a disposable projection.
- Generated full datasets, Docker volumes, checkpoints, and Parquet remain outside Git.

## Evidence required after every subgate

- code revision, physical run ID, logical data identity, input manifest, and checksums;
- exact truth counts and reconciliation equations;
- runtime, records per second, files, bytes, and timing boundaries;
- CPU, memory, disk, state, lag, offsets, WAL, or database sizes as applicable;
- idempotency or restart result;
- errors and retries, including unsuccessful attempts;
- cost, which is expected to be USD 0 for this local gate;
- limitations and explicit claims that the data is controlled replay rather than one million NASA observations.

Only compact Markdown/JSON evidence and representative fixtures enter Git.

## Completion criteria

Phase 6G passes only when all six subgates pass in order, all 49 current tests plus new tests pass, counts reconcile, quality and performance evidence is committed, actual local cost is recorded, limitations are documented, and all services are stopped. A partial pass is reported by subgate and does not advance the official scale gate.

## Program sequence after the one-million gate

The standing approval through Phase 10 authorizes work but does not bypass milestone stops:

1. Phase 7 - Airflow orchestration and operational metadata for the proven vertical slice.
2. Phase 8 - FastAPI read layer, dashboard, caching boundary, and user-facing geospatial analytics.
3. Phase 9 - AWS infrastructure, least-privilege deployment, EMR Serverless execution, monitoring, budget controls, and the 5-million gate.
4. Phase 10 - 10-million final gate, production hardening, CI/CD, disaster recovery evidence, portfolio polish, resume bullets, and interview guide.

Each phase receives its own architecture or execution plan before implementation. Phase 9 cannot begin paid workloads until an AWS budget and teardown plan exist.
