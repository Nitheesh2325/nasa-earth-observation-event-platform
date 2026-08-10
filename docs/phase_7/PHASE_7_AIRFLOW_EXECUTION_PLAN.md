# Phase 7 - Airflow Orchestration Execution Plan

## Status

Approved for implementation by the owner's explicit Phase 7 instruction. Architecture remains locked.

## Objective

Add one production-style Airflow DAG that orchestrates the proven local batch vertical slice without changing any Phase 1-6 data contract or processing module.

## Runtime decision

- Apache Airflow `3.3.0`, the maintained release at implementation time.
- Python 3.12 using the official Airflow 3.3.0 no-provider constraints file.
- A separate ignored `.venv-airflow` environment prevents Airflow application dependencies from changing the verified project `.venv`.
- TaskFlow APIs from `airflow.sdk`; no provider package, plugin, dynamic DAG framework, UI change, or additional service.
- Local Airflow metadata uses ignored SQLite only for the bounded integration gate. It is not a production metadata-database claim.

Official references:

- `https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html`
- `https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html`
- `https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html`
- `https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html`

## DAG contract

The single DAG ID is `nasa_eo_batch_vertical_slice_v1` with this fixed dependency chain:

```text
initialize_run
  -> nasa_extraction
  -> canonical_transformation
  -> controlled_replay
  -> spark_processing
  -> gold_generation
  -> postgres_load
  -> verification
  -> finalize_run
```

The DAG is manually triggered for Version 1.0. `schedule=None` prevents accidental NASA API calls or scale execution. Backfill safety is provided by an explicit logical date, stable orchestration identity, one active run, immutable stage receipts, and idempotent success reuse. Historical execution uses explicitly triggered logical dates rather than an implicit catch-up schedule.

## Parameters

- `gate_size`: positive integer, maximum 1,000,000.
- `source_detection_count`: positive integer, maximum 10,000.
- `replay_factor`: positive integer, maximum 100.
- `execution_profile`: `local` or `integration`.
- `source_date`: ISO date.

The initializer requires `source_detection_count * replay_factor = gate_size`. The integration profile is bounded to at most 100 records and cannot be used for a scale claim.

## Stable identity and reruns

The orchestration run ID is SHA-256-derived from DAG ID, logical date, gate size, source detection count, replay factor, source date, execution profile, and contract version. It does not contain a wall-clock timestamp.

Each stage writes an atomic operational receipt under an ignored run directory. A repeated task for the same run and stage:

1. verifies the prior receipt identity and upstream checksum;
2. returns an idempotent no-op when the prior result is successful;
3. never overwrites a successful receipt with different content;
4. records bounded failed attempts without labeling the stage successful;
5. propagates the exception so downstream tasks remain blocked.

## Retry and timeout policy

| Task | Retries | Execution timeout |
|---|---:|---:|
| initialize | 0 | 2 minutes |
| NASA extraction | 2 | 10 minutes |
| canonical transformation | 1 | 15 minutes |
| controlled replay | 1 | 30 minutes |
| Spark processing | 1 | 120 minutes |
| Gold generation | 1 | 120 minutes |
| PostgreSQL load | 1 | 120 minutes |
| verification | 0 | 60 minutes |
| finalize | 0 | 2 minutes |

Retry delay is one minute. The DAG run timeout is eight hours. Default `all_success` trigger behavior provides failure propagation.

## Operational metadata

The run manifest records:

- orchestration contract version;
- stable orchestration run ID;
- DAG ID and Airflow run ID;
- logical date and bounded parameters;
- pipeline revision;
- stage order and state;
- attempt number, start, completion, duration, and idempotent status;
- upstream and output checksums;
- final reconciliation and run status.

Only compact evidence and representative receipts enter Git. Runtime metadata remains under `data/local/`.

## Local integration gate

`dag.test()` executes the complete nine-task topology with the integration profile and four deterministic fixture records. Each stage consumes the prior stage checksum and writes a new receipt. Verification reconciles the full chain; finalization requires all seven processing stages to be successful.

This gate proves Airflow parsing, parameters, topology, timeouts, retries, failure propagation, XCom handoff, stable identities, idempotent reruns, and operational metadata. It does not rerun the already proven NASA/Spark/PostgreSQL scale workloads and is not a new data-volume claim.

## Completion criteria

- one DAG and no equivalent duplicate;
- exact task order;
- bounded parameters, retries, and timeouts;
- unit tests for topology, identity, metadata, idempotency, and failure;
- successful full local `dag.test()` execution;
- same-identity rerun reports idempotent stage reuse;
- controlled failure blocks success and is recorded;
- full project test suite passes;
- evidence and documentation committed;
- Airflow and pipeline services stopped;
- generated runtime state remains outside Git.
