# AWS Infrastructure

## Phase 9B boundary

Two AWS-native templates enforce strict deployment order. `cloudformation/cost-controls.json` creates the account-wide budget, billing alarms, and notification topic first. Only after the subscription and controls verify may `cloudformation/foundation.json` create the private platform foundation. Both are region-locked to `us-east-1`; neither creates data, an EMR job run, RDS instance, ECS task, public endpoint, NAT gateway, or scale workload.

The stack defines:

- one account-wide $50 monthly budget with five notifications;
- $25 and $40 estimated-charge alarms and one encrypted SNS alert topic;
- one retained, rotating KMS key;
- private versioned data and log buckets with KMS encryption, public access blocks, TLS-only policies, and lifecycle controls;
- one private VPC, two private subnets, no internet route, one S3 gateway endpoint, and five temporary interface endpoints;
- one prefix-scoped EMR Serverless runtime role;
- one Spark 4.0.2 / EMR 8.0.0 application capped at 16 vCPU, 64 GB memory, and 200 GB disk with no preinitialized capacity and a 10-minute idle stop;
- encrypted CloudWatch/S3/managed logs, 30-day log retention, an error metric/alarm, and an operational dashboard.

`logs:DescribeLogGroups` and `cloudwatch:PutMetricData` are the only IAM actions using `Resource: "*"`. AWS does not support resource-level permissions for these calls. `cloudwatch:PutMetricData` is further constrained to namespace `ASTRAYAN/V1`. KMS key policies necessarily use `Resource: "*"` because the policy is attached to that single key; principals and encryption context constrain use.

Bucket default encryption is authoritative. Bucket policies deny non-TLS requests. They intentionally do not require clients to send an explicit encryption header because EMR Serverless can rely on bucket-default SSE-KMS; requiring that header would reject otherwise encrypted service writes.

## Safe workflow

1. Run `scripts/validate-foundation.ps1` locally; it validates both templates.
2. Copy `cloudformation/parameters.example.json` outside Git and replace the example email and expiry date.
3. Obtain explicit owner approval for authenticated AWS access and resource creation.
4. Confirm the target identity uses SSO/MFA, account-level CloudTrail is active, Billing metrics are enabled, and the SNS email subscription can be confirmed.
5. Run the confirmation-gated `scripts/deploy-cost-controls.ps1`, confirm the email, and require `scripts/verify-cost-controls.ps1` to pass.
6. Run the optional online validation only after authentication approval.
7. Run the confirmation-gated `scripts/deploy-foundation.ps1`; it refuses to proceed unless the $50 budget, both alarms, and SNS confirmation verify. It uses `CAPABILITY_NAMED_IAM` and required stack-level tags.
8. Run `scripts/inventory.ps1` before and after every deployment/teardown.
9. Use `scripts/teardown.ps1` only after evidence retention decisions are complete. Keep cost controls until billing settles, then use `scripts/teardown-cost-controls.ps1`; it refuses while the foundation stack exists.

The retained S3 buckets and KMS key protect against accidental evidence deletion. CloudFormation stack deletion will not empty buckets and will not schedule the KMS key for deletion. Those destructive operations require a separate, explicit owner decision after evidence acceptance.

## Data prefixes

The template outputs the governed `bronze/`, `silver/`, and `gold/` URIs. Prefixes become material only when checksum-admitted objects are written; empty marker objects are prohibited. Additional governed prefixes are `artifacts/`, `quarantine/`, and `evidence/`. The log bucket uses `emr-serverless/`.

## Configuration and secrets

- No AWS credential belongs in parameters, environment examples, CloudFormation, Git, S3 manifests, or logs.
- `NotificationEmail` is supplied at deployment and is not committed.
- Application artifacts use Git-SHA paths and recorded checksums.
- `job-run-configuration.example.json` is inert and contains placeholders only. It keeps the 60-minute execution timeout and one-attempt policy visible; it cannot submit a job as committed.
- Database secrets and ECS permissions are deferred until the approved loader/RDS implementation milestone; granting unused permissions now would violate least privilege.

## Deployment verification

Live Phase 9B verification must prove cost-control stack completion before foundation creation, confirmed budget subscription, two billing alarms, foundation stack completion, bucket public-access blocks and SSE-KMS, versioning/lifecycle, exact IAM simulation, EMR application `CREATED` or `STARTED` readiness without a job, maximum capacity, logging settings, private network routes, project tags, and before/after inventory. No 5M or 10M input is allowed.
