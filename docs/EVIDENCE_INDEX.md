# Evidence Index

## Release truth

| Claim | Primary evidence |
|---|---|
| 10,000 admitted NASA detections | [NASA 10,000 input gate](../reports/quality/NASA_10000_INPUT_GATE.md) |
| 1M deterministic replay | [Replay 1M generation gate](../reports/quality/REPLAY_1000000_GENERATION_GATE.md) |
| 1M Spark batch | [Spark 1M batch gate](../reports/quality/SPARK_1000000_BATCH_GATE.md) |
| 1M Kafka | [Kafka 1M gate](../reports/quality/KAFKA_1000000_REPLAY_GATE.md) |
| 1M Structured Streaming and recovery | [Streaming 1M gate](../reports/quality/SPARK_KAFKA_1000000_STREAMING_GATE.md) |
| 1M compact Gold | [Gold 1M gate](../reports/quality/GOLD_1000000_GATE.md) |
| 1M PostgreSQL/PostGIS | [PostGIS 1M gate](../reports/quality/POSTGIS_1000000_COMPACT_GATE.md) |
| Airflow and idempotent rerun | [Airflow integration gate](../reports/quality/PHASE_7_AIRFLOW_INTEGRATION_GATE.md) |
| FastAPI/read-only/PostGIS plans | [FastAPI gate](../reports/quality/PHASE_8A_FASTAPI_GATE.md) |
| Bounded cache | [Cache gate](../reports/quality/PHASE_8B_CACHE_GATE.md) |
| Operational status API | [Status API gate](../reports/quality/PHASE_8C1_OPERATIONAL_STATUS_GATE.md) |
| Dashboard | [Dashboard gate](../reports/quality/PHASE_8C2_DASHBOARD_GATE.md) |
| 10M generation and Spark OOM boundary | [Local 10M batch gate](../reports/quality/LOCAL_10000000_BATCH_GATE.md) |
| AWS plan, zero spend | [AWS execution plan](phase_9/PHASE_9A_AWS_EXECUTION_PLAN.md), [cost report](AWS_COST_REPORT.md) |
| AWS IaC local validation | [AWS foundation gate](../reports/quality/PHASE_9B_AWS_FOUNDATION_GATE.md) |
| Version 1.0 recovery | [Local recovery gate](../reports/quality/V1_LOCAL_RECOVERY_GATE.md) |
| Version 1.0 release | [Local release gate](../reports/quality/V1_LOCAL_RELEASE_GATE.md) |

## Dashboard screenshots

- [Mission and pipeline overview](images/dashboard-overview-v1.png)
- [Bounded geospatial explorer](images/dashboard-geospatial-v1.png)
- [Detection lineage](images/dashboard-lineage-v1.png)

## Contracts and design

- [Canonical event](../contracts/events/v1/CANONICAL_EVENT_CONTRACT.md)
- [Bronze](../contracts/bronze/BRONZE_DATA_CONTRACT.md)
- [Replay](../contracts/replay/NASA_REPLAY_V1_CONTRACT.md)
- [Kafka](../contracts/kafka/KAFKA_REPLAY_V1_CONTRACT.md)
- [Silver](../contracts/silver/SILVER_DATA_CONTRACT.md)
- [Gold](../contracts/gold/GOLD_SERVING_CONTRACT.md)
- [Architecture](../ARCHITECTURE.md)
- [Engineering decisions](ENGINEERING_DECISIONS.md)
- [Performance report](../PERFORMANCE_REPORT.md)
