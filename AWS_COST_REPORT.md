# AWS Cost Report

## Phase 9A planning baseline

**As of:** 2026-08-10  
**Region:** `us-east-1`  
**Actual AWS resources created by this project:** 0  
**Actual AWS workload executions:** 0  
**Actual AWS cost incurred by this project:** **$0.00**

Phase 9A performed local documentation and pricing research only. No AWS CLI, SDK, Console, infrastructure-as-code deployment, or AWS service API operation was executed.

## Approved cost-control boundary

- A $50 monthly AWS Budget must exist and its notification subscription must be confirmed before any billable resource is created.
- Actual notifications: 50%, 80%, and 100%.
- Forecast notifications: 80% and 100%.
- CloudWatch billing alarms: $25 and $40.
- Target combined 5M and 10M execution envelope: $33-$45 before tax and credits.
- Mandatory owner stop/review: execution-day worst-case estimate above $45 or less than $10 contingency below the $50 budget.
- Free-tier credits are not used to justify affordability; gross cost and credits must be recorded separately.

## Planning estimate

| Component | Combined planning bound |
|---|---:|
| EMR Serverless | $5-$12 |
| Temporary RDS PostgreSQL/PostGIS | $8-$16 |
| Fargate loader and verifier | <$5 |
| S3 storage and requests | <$5 |
| CloudWatch logs, metrics, alarms, dashboard | <$5 |
| Secrets Manager, KMS, and ECR | <$4 |
| Private interface endpoints | $3-$8 |
| Contingency | $8 |
| **Target total** | **$33-$45** |

These are conservative planning bounds, not AWS quotes. The execution-day AWS Pricing Calculator estimate is authoritative. Runtime, storage, request volume, logs, snapshots, regional rates, taxes, and retries can change actual cost.

## Required actual-cost ledger

Each cloud execution must append a row after billing data becomes available.

| Gate | Start/end UTC | Estimate | Accrued | Final gross | Credits | Net | Teardown verified | Evidence |
|---|---|---:|---:|---:|---:|---:|---|---|
| Phase 9A plan | 2026-08-10 | $0.00 | $0.00 | $0.00 | $0.00 | $0.00 | Not applicable; nothing created | `docs/phase_9/PHASE_9A_AWS_EXECUTION_PLAN.md` |

No future row may be marked final until Cost Explorer data has settled and the project-tagged resource inventory confirms that no unapproved hourly resource remains.

## Phase 9B local foundation checkpoint

**Actual AWS resources created:** 0

**Actual AWS jobs submitted:** 0

**Actual AWS cost:** **$0.00**

The reproducible CloudFormation foundation is implemented and locally validated. No AWS CLI, authenticated API, change set, stack, budget, alarm, bucket, endpoint, role, KMS key, log group, or EMR application has been created. Live deployment remains blocked pending explicit paid-resource approval and authenticated access.

The committed template fixes the monthly budget at $50 and defines $25/$40 billing alarms. These are definitions only until the stack is deployed and the notification subscription is confirmed.

| Gate | Start/end UTC | Estimate | Accrued | Final gross | Credits | Net | Teardown verified | Evidence |
|---|---|---:|---:|---:|---:|---:|---|---|
| Phase 9B local definition | 2026-08-10 | $0.00 local | $0.00 | $0.00 | $0.00 | $0.00 | Not applicable; nothing created | `reports/quality/PHASE_9B_AWS_FOUNDATION_GATE.md` |
