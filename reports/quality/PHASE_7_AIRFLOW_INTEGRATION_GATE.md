# Phase 7 Airflow Integration Gate

## Result

**PASSED** on 2026-08-10.

## Runtime

- Apache Airflow: 3.3.0
- Python: 3.12
- Runtime: official `apache/airflow:3.3.0-python3.12` Linux image
- Image digest: `sha256:96e99f25815f533b298a4d53f283adf5c84c27334ea16ef232777cb800bddf10`
- Image size: 662,061,731 bytes
- Local metadata: ignored SQLite, 958,464 bytes after integration and rerun
- Native Windows result: unsupported runtime reproduced at `os.register_at_fork`; no success claim

## DAG contract

- DAG ID: `nasa_eo_batch_vertical_slice_v1`
- Schedule: manual only
- Active runs: 1
- DAG timeout: 8 hours
- Trigger behavior: `all_success`
- Tasks: 9
- Ordered processing stages: 7
- Parameter gate: 4 records = 4 source detections x replay factor 1
- Integration-profile maximum: 100 records

The verified order was initialize, NASA extraction, canonical transformation, controlled replay, Spark processing, Gold generation, PostgreSQL/PostGIS load, verification, and finalization.

## Execution evidence

The admitted `dag.test()` run reached Airflow state `success` with run ID `manual__2026-08-10T22:33:21.240466+00:00`. All nine tasks succeeded. Its logged DAG creation-to-completion interval was 13.895 seconds.

The final same-logical-date rerun reached Airflow state `success` with run ID `manual__2026-08-10T22:35:32.208153+00:00`. Its logged interval was 14.034 seconds. Both mapped to stable orchestration ID `d0ccaf3e94455f2e03e76de99773f1119e5d62ea5fb958062c062c4d3b73195e`.

The ignored operational manifest reconciled:

| Check | Result |
|---|---:|
| Run status | `SUCCEEDED` |
| Expected / successful stages | 7 / 7 |
| Failed stages | 0 |
| Reconciled records | 4 |
| Unique observed stage attempt values | 1 |
| Airflow run IDs retained | 2 |
| Explicit idempotent rerun events | 1 |
| Summed stage body duration | 0.000754 seconds |
| Manifest bytes | 5,371 |
| Manifest SHA-256 | `b5e84bb9ec5767470771eae0ec7bcc79ccf1d13663b1cab41c6607b5a66e434b` |

The integration profile emits deterministic checksum-linked receipts and does not execute NASA, Spark, Gold, or PostgreSQL processing. These counts are orchestration evidence only.

## Failure, retry, and idempotency evidence

- A controlled stage exception was recorded as `FAILED` with error type, message, start, completion, duration, and attempt one, then re-raised as `StageFailure`.
- The next stage rejected execution while its upstream stage was failed.
- Retrying the failed stage produced `SUCCEEDED` at attempt two.
- A repeated successful stage returned an idempotent no-op, retained attempt one, and required the same upstream checksum.
- A conflicting immutable run identity was rejected.
- DAG policy tests verified configured retry counts, timeouts, default `all_success` propagation, manual schedule, one active run, and exact topology.

## Tests

- Project environment: 68 discovered, 65 passed, 3 skipped because Airflow is intentionally isolated.
- Official Airflow Linux image: 3 DAG tests passed.
- Combined executed assertions: 68 passed, 0 failed.

## Service and repository controls

- All transient Airflow integration containers exited through `--rm`.
- No running Docker containers remained after evidence capture.
- Airflow database, logs, operational manifests, and both virtual environments remain ignored.
- No generated dataset or secret was added to Git.
