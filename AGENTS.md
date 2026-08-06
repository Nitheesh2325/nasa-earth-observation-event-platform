# NASA Earth Observation Event Intelligence Platform

PRIMARY OBJECTIVE

This repository exists to produce a flagship production-grade Data Engineering portfolio project that demonstrates senior-level engineering practices.

Every recommendation must improve one or more of:

• Technical depth
• Production readiness
• Recruiter appeal
• Resume quality
• GitHub quality
• Interview performance
• Long-term maintainability

If multiple solutions exist, recommend the one that best strengthens the portfolio unless explicitly instructed otherwise.

## Mission

Build a professional batch and streaming Data Engineering platform that processes approximately 10 million NASA-derived and explicitly labeled synthetic Earth Observation events.

## Career Target

U.S. Data Engineer / Cloud Data Engineer roles.

## Core Stack

- Python
- PySpark
- Apache Spark
- Spark Structured Streaming
- Apache Kafka
- PostgreSQL
- PostGIS
- Docker Compose
- AWS S3
- AWS EMR Serverless
- AWS CloudWatch
- Parquet
- SQL
- GitHub Actions
- Streamlit or Power BI

## Architecture

Official NASA source
→ Python extraction
→ Bronze raw storage
→ controlled replay and synthetic generation
→ Kafka
→ Spark validation, deduplication, and enrichment
→ Silver Parquet
→ Spark aggregations
→ Gold datasets
→ PostgreSQL/PostGIS
→ dashboard

## Data Integrity Rules

Never describe all 10 million records as original NASA observations.

Every event must include:

- source_type
- source_dataset
- source_record_id
- is_synthetic
- ingestion_run_id
- event_timestamp
- ingestion_timestamp

Clearly distinguish:

- original NASA records
- enriched records
- replay events
- synthetic scale-test records

## Scale Gates

Process records in this order:

1. 10,000
2. 100,000
3. 1,000,000
4. 5,000,000
5. 10,000,000

Do not advance until:

- tests pass
- counts reconcile
- quality metrics are recorded
- runtime is recorded
- limitations are documented

## Engineering Rules

- Inspect before modifying.
- Complete one milestone at a time.
- Stop and report after each milestone.
- Do not perform open-ended improvements.
- Do not rewrite successful modules unnecessarily.
- Ask before adding major dependencies.
- Do not rename the project or directories without approval.
- Never fabricate successful tests.
- Never commit secrets.
- Never commit the full generated dataset.
- Commit small representative samples only.
- Preserve stable milestones through Git.

## Spark Rules

- Use explicit schemas.
- Use DataFrame APIs.
- Avoid unnecessary collect operations.
- Validate nulls, ranges, and corrupt records.
- Deduplicate using stable keys.
- Write partitioned Parquet.
- Record runtime and throughput.
- Use checkpoints and watermarks for streaming where appropriate.

## Kafka Rules

- Use Docker Compose locally.
- Use KRaft.
- Define topics and partitions explicitly.
- Use stable event keys.
- Implement acknowledgments and bounded retries.
- Maintain rejected-event and dead-letter topics.
- Track throughput and consumer lag where practical.

## AWS Rules

- Do not deploy before the local vertical slice passes.
- Use S3 Bronze, Silver, and Gold prefixes.
- Use EMR Serverless for final Spark execution.
- Use least-privilege IAM.
- Create a budget before running workloads.
- Use one region.
- Record actual cost.
- Delete unnecessary resources.
- Never commit credentials.

## Required Documentation

Maintain:

- PROJECT_STATE.md
- README.md
- ARCHITECTURE.md
- DATA_DICTIONARY.md
- ENGINEERING_DECISIONS.md
- PERFORMANCE_REPORT.md
- AWS_COST_REPORT.md
- INTERVIEW_GUIDE.md
- CHANGELOG.md

## AI Collaboration

ChatGPT/Codex is the primary builder.

Claude or GLM may review architecture, documentation, commits, or targeted diffs.

Never allow multiple AI agents to rewrite the repository simultaneously.

## First Assignment

Do not write code or install software.

First:

1. Inspect the workspace.
2. Propose the repository structure.
3. Recommend one official NASA dataset and access method.
4. Define the event schema.
5. Define Kafka topics and partition keys.
6. Define Bronze, Silver, and Gold contracts.
7. Define Spark batch and streaming flows.
8. Define the PostgreSQL/PostGIS model.
9. Define the testing plan.
10. Define AWS resources and cost controls.
11. Define Phase 1 completion criteria.
12. Identify laptop resource risks.

Return the architecture proposal and stop for approval.