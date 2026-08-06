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
