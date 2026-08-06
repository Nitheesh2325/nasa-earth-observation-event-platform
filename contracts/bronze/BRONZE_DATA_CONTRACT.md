# Bronze Data Contract

## Purpose

Bronze is the immutable, auditable entry layer for official NASA source files, canonical event payloads prepared for replay, Kafka landing data, and run manifests.

Bronze preserves what arrived. It does not claim that source data is valid or trusted.

## Local layout

```text
data/local/bronze/
  nasa_firms/
    source_dataset=<dataset>/
      ingestion_date=<yyyy-mm-dd>/
        ingestion_run_id=<id>/
  canonical_events/
    source_type=<type>/
      ingestion_date=<yyyy-mm-dd>/
  kafka_landing/
    topic=<topic>/
      ingestion_date=<yyyy-mm-dd>/
        hour=<hh>/

data/local/manifests/
  ingestion/
  replay/
  synthetic_generation/
```

The AWS layout must preserve the same logical boundaries under approved S3 prefixes.

## Content classes

### Official source objects

- Original NASA response bytes
- Original filenames or deterministic platform filenames
- No in-place edits
- Associated request manifest and checksum

### Canonical replay input

- Canonical event messages derived from approved source objects
- Explicit source classification
- Traceable raw object and row identity

### Kafka landing records

- Event payload
- Topic
- Partition
- Offset
- Kafka timestamp
- Landing timestamp

### Manifests

- Ingestion manifests
- Replay manifests
- Synthetic-generation manifests
- Run status and reconciliation metadata

## Required partition boundaries

Bronze paths must distinguish:

- Source dataset
- Ingestion date
- Ingestion run
- Original, replay, and synthetic content where applicable
- Kafka topic and landing hour where applicable

High-cardinality event IDs must not become directory partitions.

## Immutability

- A successful Bronze object must not be modified in place.
- A corrected source retrieval creates a new ingestion run.
- A revised NASA product creates a new source object and manifest.
- Failed temporary downloads must not be presented as successful Bronze objects.
- Bronze deletion requires explicit retention policy and approval.

## Manifest linkage

Every official source object must link to exactly one ingestion manifest. The manifest must provide its object location, checksum, byte count, source request, record count when readable, and run status.

Every replay or synthetic dataset must link to its generation manifest and parameters.

## Security

- Credentials and API keys must never appear in Bronze.
- Request metadata must redact secrets.
- Local Bronze data is excluded from Git.
- AWS Bronze prefixes must block public access and use encryption.
- Logs must use identifiers instead of full payloads where possible.

## Quality boundary

Bronze may contain:

- Nulls
- Unexpected values
- Duplicate source rows
- Corrupt rows preserved inside an otherwise valid source object
- New source fields

These conditions are assessed when producing Silver. Bronze must not silently correct them.

## Acceptance criteria

Bronze is valid when:

- The raw object is byte-preserved.
- Its checksum is recorded and verifiable.
- Its source request and run are identifiable.
- Its content class is unambiguous.
- It is excluded from Git when generated locally.
- No credential is present.
- A repeated extraction produces a new auditable run or an explicit idempotent no-op.

