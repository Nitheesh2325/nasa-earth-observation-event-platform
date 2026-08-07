# Engineering Decisions

## ED-001: Use the Python standard library for initial FIRMS extraction

**Status:** Accepted

**Context:** The first extraction requires bounded HTTP access, CSV contract validation, checksums, immutable local writes, manifests, retry control, and secret-safe behavior.

**Decision:** Implement the initial extractor with Python 3.12 standard-library modules. Add no external runtime dependency until a demonstrated requirement justifies one.

**Consequences:** The initial environment remains small and auditable. Retry, environment-file parsing, and atomic output behavior remain explicit project responsibilities.

## ED-002: Preserve raw source objects and manifests outside Git

**Status:** Accepted

**Context:** Bronze data must remain immutable and auditable, while the repository must not contain downloaded or generated full datasets.

**Decision:** Write local raw data and run manifests beneath `data/local/`, exclude them from Git, and commit compact reconciliation evidence instead.

**Consequences:** The repository stays small and safe. Reproducing a raw run requires the private NASA credential and the documented bounded request.

## ED-003: Use versioned deterministic source identities

**Status:** Accepted

**Context:** NASA FIRMS CSV records do not expose a universally guaranteed record UUID. Batch, replay, Kafka, and streaming paths require stable deduplication and lineage keys.

**Decision:** Build `nasa-firms-viirs-v1` identities from normalized dataset, satellite, acquisition date/time, latitude, longitude, and source product version, then SHA-256 hash the deterministic serialization.

**Consequences:** Equivalent numeric formatting produces the same identity, source-product revisions remain distinguishable, and every processing path can implement parity tests against one documented algorithm. Changing these semantics is a breaking contract change.

## ED-004: Align local Spark with the selected EMR Serverless runtime

**Status:** Accepted

**Context:** Local development must remain reproducible and should minimize incompatibilities when the final batch workloads move to EMR Serverless. The workstation already provides Python 3.12 and JDK 17.

**Decision:** Pin local PySpark to version 4.0.2 and target AWS release `emr-spark-8.0.0`. Install it only in a project-local virtual environment after owner approval. Add no optional Spark dependencies until a concrete feature requires them.

**Consequences:** Local Spark matches the upstream version used by the target EMR runtime, Python 3.12 and JDK 17 remain valid across both environments, and the initial dependency surface stays small. Amazon-specific patches mean exact runtime identity is impossible locally, so cloud parity still requires measured validation.

## ED-005: Build the first scale gate from a deterministic NASA subset

**Status:** Accepted

**Context:** The first governed scale gate requires exactly 10,000 truthful, reproducible records. A global daily VIIRS response is expected to exceed that count, but API ordering and daily availability must not determine benchmark reproducibility implicitly.

**Decision:** Acquire one fixed, availability-verified historical day of `VIIRS_SNPP_SP` for the world, canonicalize the complete response, order accepted unique events by stable `event_id`, and select the first 10,000. Preserve the complete raw object and a selection manifest outside Git. Fail rather than duplicate records if fewer than 10,000 valid unique events exist.

**Consequences:** The gate is reproducible, lineage-preserving, and truthfully composed of original NASA-derived observations. Selection bias toward event-ID ordering is acceptable for an engineering throughput gate but must be disclosed; it is not a statistically representative scientific sample.

## ED-006: Execute local Spark file workloads in an official Linux container

**Status:** Accepted

**Context:** PySpark 4.0.2 launches natively on Windows, but its Hadoop local filesystem cannot write Parquet without Windows-specific support that is not bundled with the official distribution. The repository path also exposes Windows batch-script parsing limitations.

**Decision:** Keep the pinned project-local PySpark dependency for contract development and version visibility, but execute local Spark file workloads in the official `apache/spark:4.0.2-python3` Linux image pinned by digest. Do not add an unofficial `winutils.exe` binary or rename the repository.

**Consequences:** Local execution uses Linux filesystem semantics closer to EMR Serverless, the runtime is reproducible, and untrusted Windows binaries are avoided. Docker startup and bind-mount overhead must be excluded or disclosed in performance measurements, and the container's Python runtime must be recorded.

## ED-007: Model Spark batch results as mutually exclusive governed outcomes

**Status:** Accepted

**Context:** Trusted Silver must not silently include invalid or duplicate messages, and performance evidence is meaningless when dropped records are not accounted for.

**Decision:** Every batch input receives exactly one Spark outcome: accepted, rejected, or duplicate. Use explicit schema parsing and DataFrame validation expressions, deduplicate accepted candidates by stable `event_id`, physically separate quarantine outputs, and require both pre-write and Parquet read-back reconciliation.

**Consequences:** Counts remain auditable and invalid data is diagnosable without contaminating Silver. Additional count actions increase small-run latency, which is an intentional correctness cost and must be visible in performance reports.

## ED-008: Use controlled replay for the 100,000-record gate

**Status:** Accepted

**Context:** The platform needs to advance from 10,000 to 100,000 event messages without misrepresenting scale-test volume as new NASA observations. The next milestone should also exercise lineage, ordering, Kafka partition semantics, and event-message identity.

**Decision:** Derive exactly ten deterministic `NASA_REPLAY` messages from each of the 10,000 admitted original NASA events. Preserve original detection identity, observation time, measurements, and raw lineage; assign versioned replay event identities and a separate deterministic scheduled replay timestamp. Use `lineage_root_id` as the future Kafka key.

**Consequences:** The gate truthfully represents 100,000 replay messages backed by 10,000 unique NASA detections and adds no fabricated observation measurements. Replay distribution remains limited to the original sample, so later million-scale gates may require separately labeled synthetic data for broader controlled distributions.

## ED-009: Separate logical replay identity from physical artifact paths

**Status:** Accepted

**Context:** Governed replay IDs use a URI-like colon-delimited identity that is valid in event content but invalid in Windows directory names. Long descriptive partition labels also exceeded the workstation's path-length boundary.

**Decision:** Preserve the complete `nasa-replay-v1:sha256:<digest>` identity in every event and manifest. Use only filesystem-safe `plan=<digest>/run=<execution-uuid>` components for local physical paths.

**Consequences:** Logical identities remain portable and deterministic while local paths work across Windows and Linux. Consumers must use manifests rather than infer complete logical identities from directory names.

## ED-010: Use official Kafka 4.3.1 KRaft locally with a production-grade Python client

**Status:** Accepted

**Context:** The streaming milestone requires explicit KRaft infrastructure, reliable producer delivery, Spark 4.0.2 connector compatibility, and strict laptop controls. A local multi-node cluster would add resource pressure without improving the first vertical-slice evidence.

**Decision:** Use the official `apache/kafka:4.3.1` image as one combined broker/controller for local development, `confluent-kafka==2.15.0` for Python production and diagnostic consumption, and `spark-sql-kafka-0-10_2.13:4.0.2` for Spark. Cap Kafka at 1.5 GiB and two CPUs, disable automatic topic creation, and create bounded topics explicitly. Document a separate production topology with dedicated controllers, multiple brokers, replication factor three, TLS, authentication, and authorization.

**Consequences:** Local streaming remains feasible on the 16-GB laptop while using current production-relevant components. The single node provides no broker high availability and cannot validate replication or failover; those limitations must remain explicit in portfolio claims.

## ED-011: Key Kafka replay messages by lineage root

**Status:** Accepted

**Context:** Replay events have unique event IDs but share an underlying original NASA detection. The streaming system needs deterministic per-lineage ordering without claiming global order across partitions.

**Decision:** Use `lineage_root_id` as the UTF-8 Kafka key and retain `event_id` as the unique deduplication identity. Configure six replay-topic partitions locally.

**Consequences:** All ten replay messages for a detection remain in one ordered partition, while different lineages can process concurrently. Load balance depends on the hash distribution of 10,000 lineage keys and must be measured per partition.

## ED-012: Pin the local broker image by digest and advertise IPv4 explicitly

**Status:** Accepted

**Context:** A mutable image tag weakens reproducibility. On this Windows host, Kafka's advertised `localhost` address resolved to IPv6 even though the Compose port was intentionally bound only to IPv4, causing the first pre-publication watermark request to time out.

**Decision:** Pin `apache/kafka:4.3.1` to digest `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`, bind the host port to `127.0.0.1`, and advertise `127.0.0.1:9092` to host clients. Preserve the internal Docker-network listener separately.

**Consequences:** Local broker identity is immutable and Windows clients do not depend on hostname address-family selection. The host listener remains deliberately local and plaintext; it must not be presented as the production security topology.

## ED-013: Separate streaming outcomes by query and checkpoint

**Status:** Accepted

**Context:** Kafka Bronze landing, trusted deduplicated Silver admission, and invalid-event quarantine have different state and recovery semantics. Sharing a checkpoint across query shapes is unsafe, while ad hoc multi-sink writes can duplicate data on retry.

**Decision:** Use one explicit-offset source boundary but three query identities and checkpoint paths: append-only Kafka Bronze, watermark-bounded accepted-event deduplication, and rejected-event quarantine. Use `scheduled_replay_timestamp` as replay event time, a ten-minute watermark, and `event_id` as the deduplication key. Preserve physical execution manifests immutably beneath the logical streaming run ID.

**Consequences:** Each sink recovers independently and the bounded fixture proves zero-input restart behavior. Kafka is read three times, which adds local overhead. Duplicate count is reconciled as landed minus accepted minus rejected; a later production design must decide whether duplicate payloads require their own physical audit sink.

## ED-014: Cache connector artifacts for compatibility, then bake them before scale evidence

**Status:** Accepted

**Context:** Python Spark applications require the Kafka connector and its dependencies. Network resolution during a measured scale run would weaken reproducibility and mix download latency with processing evidence.

**Decision:** Resolve `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2` once for the bounded compatibility fixture, record every resolved JAR name, size, and SHA-256, and keep binaries outside Git. Before the full streaming gate, build and digest-pin a derived image that contains exactly those artifacts and starts without network resolution.

**Consequences:** Compatibility is proven now without claiming the runtime is final. The full gate remains blocked until the derived image is reproducible and pinned.

## ED-015: Pin the derived runtime to a single-platform manifest

**Status:** Accepted

**Context:** Docker BuildKit's default provenance attestation can change the top-level manifest-list digest between otherwise byte-identical local builds. The application image manifest, configuration, and layers remained identical, but the generated attestation envelope did not.

**Decision:** Build this local Linux/AMD64 runtime with `--provenance=false`, retain checksum validation for every remote JAR, and pin the resulting single-platform image digest `sha256:d92fdb4dc4cc1febc451308ea17880f48b511f65528cc792120a2345b9d6fff3`. Require full streaming runs to omit runtime package resolution.

**Consequences:** Consecutive local builds have one stable image identity and the performance gate cannot download dependencies at startup. This local digest is not a registry distribution claim; publishing later requires a controlled registry and separate provenance/signing policy.

## ED-016: Preserve the first full streaming output before compaction

**Status:** Accepted

**Context:** The full gate produced 161 Silver Parquet files for 100,000 rows because 16 shuffle partitions were written across ten stateful microbatches. Rewriting the successful gate output would erase direct evidence and mix processing with optimization.

**Decision:** Preserve the immutable Phase 5F output and report the small-file result as measured evidence. Design a separate idempotent compaction stage later with explicit target file sizes and before/after metrics.

**Consequences:** The portfolio demonstrates detection of a realistic streaming lakehouse problem instead of hiding it. Downstream consumers should use a compacted Gold or serving layer rather than treating the raw streaming file layout as optimized.

## ED-017: Isolate deterministic fault injection from production routing

**Status:** Accepted

**Context:** Dead-letter behavior cannot be proven without a repeatable exhausted-processing failure, but silently embedding random or always-active failures would make production semantics unsafe and evidence irreproducible.

**Decision:** Permit the `_test_fault_mode=EXHAUST_RETRIES` marker only in a committed bounded fixture and require the explicit `--enable-test-fault-injection` CLI flag. Execute the processing function exactly up to the configured attempt count, then preserve the actual count and bounded failure category in the DLQ envelope.

**Consequences:** The three-attempt DLQ path is deterministic and auditable. This fixture proves routing mechanics, not a real external-service outage or distributed retry scheduler; production deployment must omit the test flag and use classified operational exceptions.

## ED-018: Treat PostgreSQL as a rebuildable Gold serving projection

**Status:** Accepted

**Context:** FastAPI and dashboard users need low-latency relational and spatial queries, but loading raw streaming output directly into PostgreSQL would duplicate source-of-truth responsibilities and couple recovery to the database.

**Decision:** Keep compacted Gold Parquet and its immutable manifest authoritative. Load selected event detail and aggregate products into PostgreSQL/PostGIS through an idempotent, staged, reconciled transaction. Do not write from Kafka or FastAPI directly to serving tables.

**Consequences:** PostgreSQL can be rebuilt, API workloads remain isolated from Spark processing, and database loss does not erase analytical truth. Gold transformation and a governed loader become required boundaries.

## ED-019: Begin with an unpartitioned 10-million-row event table

**Status:** Accepted

**Context:** Native range partitioning complicates global `event_id` uniqueness, and controlled replay retains original event time, which can concentrate many event messages in one date partition. Ten million rows is within a measured PostgreSQL serving-table envelope.

**Decision:** Use one unpartitioned `serving.event_detail` table with a global primary key, targeted B-tree, GiST, and BRIN indexes. Reconsider partitioning only after measured growth, maintenance, or query evidence justifies the added complexity.

**Consequences:** Idempotency and conflict detection stay simple, while the project demonstrates evidence-based partition decisions. Full scans still require bounded API queries and preaggregated Gold products.

## ED-020: Align local PostgreSQL/PostGIS with the RDS target

**Status:** Accepted

**Context:** PostgreSQL 18 is current, but Amazon RDS extension availability is engine-minor-specific. Local use of a newer major or PostGIS line would weaken cloud parity without improving the first serving gate.

**Decision:** Target PostgreSQL 16 and the RDS-compatible PostGIS 3.4 line. At implementation, pin the current supported PostgreSQL minor, exact PostGIS patch, and local image digest, then record the independently reported runtime versions.

**Consequences:** Local and cloud behavior remain comparable and PostgreSQL 16 retains upstream support through November 2028. Minor and extension upgrades remain explicit compatibility events rather than floating changes.
