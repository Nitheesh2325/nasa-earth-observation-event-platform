# Local 10,000,000-Event Batch Gate

## Result

**Status:** Blocked by verified local Spark JVM heap limit

**Execution date:** 2026-08-11

**Repository baseline:** `4d2ff4ff105e4d137b7a8efd9691920cad5dab73`

**Cloud activity and cost:** none; USD 0.00

The deterministic replay-generation and independent replay-verification portions passed. The Spark batch portion failed safely with `java.lang.OutOfMemoryError: Java heap space` before any Silver or quarantine output was written and before any success manifest existed. No retry with a larger local heap was attempted because Docker Desktop exposes only 7.65 GiB total and the milestone explicitly requires stopping at a verified OOM risk.

This workload is 10,000 original NASA detections replayed exactly 1,000 times. It is not 10,000,000 original NASA observations and contains no synthetic events.

## Source admission

| Check | Result |
|---|---:|
| Admitted NASA-original source rows | 10,000 |
| Source dataset | `VIIRS_SNPP_SP` |
| Source file lines | 10,000 |
| Source SHA-256 | `3f711f63138ae1e5c926d5dcd6edf3a15094ae8f7d8631db4e566b936862ede3` |
| Pre-run automated discovery | 108 run; 100 passed; 8 environment-gated skips |

## Deterministic replay generation

| Metric | Execution 1 | Execution 2 |
|---|---:|---:|
| Physical execution ID | `1467217c-9a64-4816-90ed-06dd7f17c8d9` | `63a842e2-fdaa-42bf-9516-534cee34d979` |
| Logical replay run ID | `nasa-replay-v1:sha256:298bd9568f160aceb5d84e8cc0386e8c0456c6a107d4162e805e2522c27b6464` | identical |
| Output rows | 10,000,000 | 10,000,000 |
| Output bytes | 18,445,760,890 | 18,445,760,890 |
| Output SHA-256 | `50096a05162ead1c72b879997d23bc16dc382905e8eea937b7e1ee30548952cc` | identical |
| Generator duration | 490.562 s | 461.594 s |
| Command wall time | 492.072 s | 462.983 s |
| Generator throughput | 20,384.79 events/s | 21,664.05 events/s |
| Highest sampled process memory | approximately 1.54 GiB | approximately 1.15 GiB |

Both physical artifacts remain under ignored `data/local/bronze/replay_events_10m/`. Their combined data-file footprint is 36,891,521,780 bytes. Only compact manifests and this report are eligible for Git; the local manifests also remain ignored.

## Independent replay verification

| Check | Result |
|---|---:|
| Status | Passed |
| Parsed rows / unique event IDs | 10,000,000 / 10,000,000 |
| Unique underlying NASA detections | 10,000 |
| Events per detection | exactly 1,000 |
| Source type | 10,000,000 `NASA_REPLAY` |
| Synthetic true | 0 |
| Replay sequence | exactly 0-9,999,999 |
| Replay iterations | exactly 1-1,000 |
| First scheduled timestamp | `2026-08-11T00:00:00.000Z` |
| Last scheduled timestamp | `2026-08-12T03:46:39.990Z` |
| Recomputed bytes | 18,445,760,890 |
| Recomputed SHA-256 | `50096a05162ead1c72b879997d23bc16dc382905e8eea937b7e1ee30548952cc` |
| Verification duration | 1,819.625 s |
| Verification throughput | 5,495.64 events/s |
| Sampled verifier working set | approximately 74 MiB |

The independent verifier reparsed the complete artifact, recomputed every event identity and schedule position, checked lineage and preserved source fields, reconciled detection frequency and classification, and hashed the raw stream. It did not trust the generator's counters.

## Spark batch attempt

| Property | Value |
|---|---|
| Runtime | Spark 4.0.2, Java 17, `local[4]` |
| Image | `apache/spark:4.0.2-python3` |
| Image digest | `sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3` |
| CPUs | 4 |
| Container memory / swap | 5 GiB / 5 GiB |
| Driver heap | 3 GiB |
| Driver result limit | 512 MiB |
| Shuffle partitions | 128 |
| Input rows / bytes | 10,000,000 / 18,445,760,890 |
| Approximate time to failure | 629 seconds |
| Exit code | 1 |
| Failure | `java.lang.OutOfMemoryError: Java heap space` |
| Failure location | Cached classified DataFrame columnar materialization in stage 2 |
| Early sample | 1.009 GiB / 5 GiB; 182.48% CPU; 231 MB read / 743 MB written |
| Later sample | 1.37 GiB / 5 GiB; 200.09% CPU; 231 MB read / 1.06 GB written |
| Exact peak | unavailable; sampled metrics missed the terminal heap spike |

Spark emitted repeated GCLocker allocation warnings and then heap errors in multiple tasks. Spark aborted the task set after the first failure and the container exited nonzero. The container-level memory limit was not reported as an engine OOM kill; the application JVM exhausted its configured 3 GiB heap.

## Spark reconciliation

| Required outcome | Result |
|---|---|
| Accepted rows | Not produced; job failed before write |
| Rejected rows | Not produced; job failed before write |
| Duplicate rows | Not produced; job failed before write |
| Silver read-back | Not run; no Silver output exists |
| Unique Silver event IDs | Not available |
| Underlying Silver detections | Not available |
| Silver replay iterations/classification | Not available |
| Spark success manifest | Not created |
| Spark output files / bytes | 0 / 0 |

No partial Spark directory or manifest remained under the 10M gate paths. Therefore no failed output can be mistaken for admitted Silver evidence.

## Resource and safety verification

- Free D: disk before the milestone: 982,441,074,688 bytes.
- Free D: disk after generation, verification, and failed Spark cleanup: 945,549,541,376 bytes.
- Generated replay data files: 2 files, 36,891,521,780 bytes, outside Git.
- Generated replay manifest files: 2 files, 3,876 bytes, outside Git.
- Generated Spark output/manifest files: 0.
- Docker memory available: 8,211,017,728 bytes (approximately 7.65 GiB).
- Running containers after failure: 0.
- Kafka remained stopped; PostgreSQL, Airflow, API, dashboard, and AWS were not started.
- The failed `--rm` Spark container was removed automatically.
- Git showed no generated 10M data candidates.

## Limitations and gate decision

- The local 10M deterministic replay-generation claim passes.
- The local 10M Spark batch claim does not pass.
- Consequently, the complete requested 10M batch milestone is blocked, not complete.
- Increasing the JVM heap would compete directly with Docker Desktop's 7.65-GiB ceiling and the 16-GB Windows host. The verified heap failure satisfies the required stop condition; an unmeasured larger-memory retry would create OOM and corruption risk.
- This single-laptop, mechanical-drive attempt is not a distributed Spark benchmark.
- The 30-minute replay verifier demonstrates that mechanical storage and repeated full JSON parsing materially constrain local wall time.
- No Kafka, Structured Streaming, PostgreSQL, FastAPI, dashboard, Airflow, or AWS 10M claim is made.

The repository remains at the verified one-million end-to-end platform gate. Ten-million deterministic replay generation is separately proven, while ten-million Spark processing requires a higher-memory execution environment or an explicitly approved bounded implementation change; neither is authorized by this milestone.
