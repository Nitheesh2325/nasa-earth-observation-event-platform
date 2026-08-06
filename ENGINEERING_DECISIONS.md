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
