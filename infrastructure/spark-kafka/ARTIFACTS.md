# Spark-Kafka Runtime Artifact Lock

## Base image

`apache/spark@sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3`

## Added artifacts

| Maven coordinate | Bytes | SHA-256 |
|---|---:|---|
| `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2` | 467,924 | `faa232c017b0cb4480c14643eb82abf6d0cac4b4d032059bf999b9246d045eb6` |
| `org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.2` | 64,979 | `cc5ccc2f8d0d600cb1c356f3de21fb8b21cfaae16e73e350f4fd25a2d727a7ce` |
| `org.apache.kafka:kafka-clients:3.9.1` | 9,215,194 | `7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9` |
| `org.apache.commons:commons-pool2:2.12.0` | 150,048 | `6d3bd18df8410f3e31b031aca582cc109342358a62a2759ebd0c4cdf30d06f8b` |

Docker BuildKit verifies each remote download against its declared SHA-256 before committing the layer.

## Verified base-image artifacts

These seven transitive dependencies are already present in the base image. Their base-image checksums exactly match the artifacts resolved during Phase 5D, so the derived image does not add duplicate copies.

| Artifact | SHA-256 |
|---|---|
| `hadoop-client-api-3.4.1.jar` | `a964d4daa054c9615bbafb4553efbb140fa7fb9ac6f358a24393f183a5703438` |
| `hadoop-client-runtime-3.4.1.jar` | `f6a800a159f918670db533606d33560d6c13b7e13f14eda493280ae33b9eeb2f` |
| `jsr305-3.0.0.jar` | `bec0b24dcb23f9670172724826584802b80ae6cbdaba03bdebdef9327b962f6a` |
| `lz4-java-1.8.0.jar` | `d74a3334fb35195009b338a951f918203d6bbca3d1d359033dc33edd1cadc9ef` |
| `scala-parallel-collections_2.13-1.2.0.jar` | `4eae6e68cf44e9f709970355590ae981883edf6484608d747376a56cbb285432` |
| `slf4j-api-2.0.16.jar` | `a12578dde1ba00bd9b816d388a0b879928d00bab3c83c240f7013bf4196c579a` |
| `snappy-java-1.1.10.7.jar` | `4c766cb3f855415ee734b2392949a0b6f12a60879334a74518deaf6270d32e36` |

## Reproducibility rule

The full Structured Streaming gate must reference the derived image by immutable digest and must not pass `--packages`, `--jars`, or an Ivy cache path at runtime.

## Derived image

- local name: `nasa-eo-spark-kafka:4.0.2-v1`
- immutable single-platform digest: `sha256:d92fdb4dc4cc1febc451308ea17880f48b511f65528cc792120a2345b9d6fff3`
- local unpacked size: 795,890,344 bytes
- runtime user: `spark`

Build with generated provenance disabled so Docker's changing attestation envelope does not alter the local top-level identity:

```text
docker build --pull=false --provenance=false --tag nasa-eo-spark-kafka:4.0.2-v1 infrastructure/spark-kafka
```

Two consecutive builds produced the same image digest and size. The remote artifacts remain checksum-enforced by the Dockerfile.
