# Spark-Kafka Structured Streaming Fixture Gate

## Result

**Passed** on 2026-08-07. This is a three-message compatibility, correctness, and checkpoint-recovery fixture. It is not the 100,000-message streaming gate and not a throughput benchmark.

## Official compatibility basis

The [Apache Spark 4.0.2 Kafka integration guide](https://spark.apache.org/docs/4.0.2/structured-streaming-kafka-integration.html) specifies `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2` and requires Python deployments to include the library and its dependencies. Spark's [Structured Streaming guide](https://spark.apache.org/docs/4.0.2/streaming/getting-started.html) documents checkpointed offset tracking and restart recovery. Spark's [Dataset API](https://spark.apache.org/docs/4.0.2/api/scala/org/apache/spark/sql/Dataset.html) documents `dropDuplicatesWithinWatermark` for bounded streaming deduplication.

## Environment

| Item | Value |
|---|---|
| Pipeline implementation revision | `d0e535e` |
| Immutable-manifest correction | `caef373` |
| Spark | 4.0.2, `local[4]` |
| Base image digest | `sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3` |
| Spark container limit | 4 CPUs, 3 GiB |
| Driver memory | 2 GiB |
| Kafka | 4.3.1 local single-node KRaft |
| Connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2` |
| Shuffle partitions | 4 |
| Maximum offsets per trigger | 100 |
| Trigger | Available Now |

## Governed fixture

Fixture SHA-256: `f25689c6d3e3536219fac944da42ea24d84b0828d9526922aa1ab67d4f6b9c2c`.

The fixture contains two valid `NASA_REPLAY` messages sharing one event ID and one invalid replay message with a missing event ID and out-of-range latitude. The producer run `b5474425-175d-4e7d-a0fc-2c310f99495e` acknowledged all three messages and reconciled an offset delta of three.

## Query architecture

| Query | Purpose | Checkpoint |
|---|---|---|
| Landing v1 | Preserve raw value plus topic, partition, offset, broker timestamp, key, and headers | `landing-v1` |
| Accepted v1 | Parse explicit schema, validate, watermark, deduplicate, and write trusted Parquet | `accepted-v1` |
| Rejected v1 | Preserve invalid events and stable reason codes in Parquet quarantine | `rejected-v1` |

The accepted query uses `scheduled_replay_timestamp`, a ten-minute watermark, and `dropDuplicatesWithinWatermark(["event_id"])`. The original observation timestamp remains scientific event time and is not used as the replay watermark.

## First execution

| Check | Result |
|---|---:|
| Kafka messages landed | 3 |
| Accepted unique events | 1 |
| Rejected events | 1 |
| Duplicate events | 1 |
| Kafka offset ranges reconciled | Yes |
| Total outcome reconciliation | 3 = 1 + 1 + 1 |
| Wall-clock job duration | 48.930 seconds |

Only partitions 0 and 5 received the three fixture keys. Observed ranges `[16480,16482)` and `[17118,17119)` exactly matched the producer manifest. Startup, three sequential streaming queries, Parquet read-back, and connector initialization dominate this tiny run.

## Checkpoint recovery

The identical logical streaming run was restarted with the same three checkpoints and output paths. Final immutable recovery execution ID: `ecc9d115-b4de-44f0-8494-a021ede3eada`.

| Recovery check | Result |
|---|---:|
| New landing source rows | 0 |
| New accepted source rows | 0 |
| New rejected source rows | 0 |
| Reported maximum Kafka lag | 0 |
| Bronze read-back after restart | 3 |
| Silver read-back after restart | 1 |
| Rejected read-back after restart | 1 |
| Reconciled duplicate count | 1 |
| Output counts changed | No |

The first implementation wrote one manifest per logical run, so the first restart replaced that local manifest. The issue did not affect checkpoints or output. Revision `caef373` changed the layout to immutable per-execution manifests, and the final recovery execution preserves explicit zero-input evidence.

## Resolved artifacts

All binaries remain under ignored `data/local/ivy/`; none are committed.

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| `spark-sql-kafka-0-10_2.13-4.0.2.jar` | 467,924 | `faa232c017b0cb4480c14643eb82abf6d0cac4b4d032059bf999b9246d045eb6` |
| `spark-token-provider-kafka-0-10_2.13-4.0.2.jar` | 64,979 | `cc5ccc2f8d0d600cb1c356f3de21fb8b21cfaae16e73e350f4fd25a2d727a7ce` |
| `kafka-clients-3.9.1.jar` | 9,215,194 | `7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9` |
| `lz4-java-1.8.0.jar` | 682,804 | `d74a3334fb35195009b338a951f918203d6bbca3d1d359033dc33edd1cadc9ef` |
| `snappy-java-1.1.10.7.jar` | 2,338,496 | `4c766cb3f855415ee734b2392949a0b6f12a60879334a74518deaf6270d32e36` |
| `slf4j-api-2.0.16.jar` | 69,435 | `a12578dde1ba00bd9b816d388a0b879928d00bab3c83c240f7013bf4196c579a` |
| `hadoop-client-runtime-3.4.1.jar` | 30,413,579 | `f6a800a159f918670db533606d33560d6c13b7e13f14eda493280ae33b9eeb2f` |
| `hadoop-client-api-3.4.1.jar` | 19,631,854 | `a964d4daa054c9615bbafb4553efbb140fa7fb9ac6f358a24393f183a5703438` |
| `jsr305-3.0.0.jar` | 33,031 | `bec0b24dcb23f9670172724826584802b80ae6cbdaba03bdebdef9327b962f6a` |
| `scala-parallel-collections_2.13-1.2.0.jar` | 1,120,803 | `4eae6e68cf44e9f709970355590ae981883edf6484608d747376a56cbb285432` |
| `commons-pool2-2.12.0.jar` | 150,048 | `6d3bd18df8410f3e31b031aca582cc109342358a62a2759ebd0c4cdf30d06f8b` |

## Limitations and next gate

- Dependency resolution used the network for this compatibility fixture; the full gate must use a derived image containing checksum-verified JARs.
- Three independent queries read the bounded Kafka range separately.
- Rejected records currently land in Parquet quarantine; rejected-topic and dead-letter publication remain unimplemented.
- Duplicate payloads are removed from trusted Silver and counted by reconciliation but do not yet have a dedicated physical streaming audit sink.
- No full 100,000-message Structured Streaming execution occurred.
