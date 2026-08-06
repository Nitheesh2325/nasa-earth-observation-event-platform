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
