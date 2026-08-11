# Phase 9B AWS Foundation Gate

## Status

**Local implementation:** Passed

**AWS service-side validation:** Not executed; authenticated AWS access is not available or authorized

**AWS resource creation:** Not executed; explicit paid-resource approval is required

**Overall Phase 9B gate:** Open pending live deployment, verification, and teardown evidence

## Baseline

- Baseline commit: `d924b275512cc723394e3a636f356b7f6ebd801e`
- Region contract: `us-east-1`
- Architecture decision: ED-034
- Actual AWS resources at this checkpoint: 0
- Actual AWS cost at this checkpoint: $0.00
- Scale workload submitted: none

## Implemented definitions

- Two AWS-native CloudFormation templates with region assertions and inert parameters enforce cost-controls-before-foundation deployment.
- $50 account-wide monthly budget with actual 50/80/100% and forecast 80/100% notifications, avoiding cost-allocation-tag activation delay or untaggable-cost gaps.
- $25 and $40 AWS/Billing alarms linked to an encrypted SNS topic.
- Required project, owner, environment, management, cost-center, gate, and expiry tags on core resources.
- Retained rotating customer-managed KMS key.
- Two private, versioned, SSE-KMS buckets with Block Public Access, bucket-owner enforcement, TLS-only policies, abort/expiration/archive lifecycle rules, and retained deletion policy.
- Governed Bronze, Silver, Gold, artifact, quarantine, evidence, and log prefix contracts.
- Private VPC, two private subnets, no internet gateway/NAT/public route, S3 gateway endpoint, and five interface endpoints.
- Prefix-scoped EMR Serverless runtime role with two documented resource-level wildcard exceptions.
- EMR Serverless Spark application pinned to `emr-8.0.0`, capped at 16 vCPU/64 GB/200 GB, no initial capacity, 10-minute idle stop, adaptive Spark, dynamic allocation capped at three executors, and 64 initial shuffle partitions.
- Encrypted managed persistence, CloudWatch logs, S3 logs, 30-day log retention, error metric/alarm, and operational dashboard.
- Reproducible local validation plus exact-confirmation deployment, authenticated inventory, and CloudFormation teardown procedures.

## Local validation evidence

| Check | Result |
|---|---|
| CloudFormation JSON parse | Pass |
| Required resource coverage | Pass |
| Region/cost controls | Pass |
| S3 privacy, encryption, versioning, lifecycle | Pass |
| IAM wildcard boundary | Pass |
| EMR capacity, private networking, logging | Pass |
| No RDS/ECS/MSK/EKS/job-run/traffic resources | Pass |
| Core resource tags | Pass |
| Local infrastructure unit tests | 8 passed |
| PowerShell parser validation | Pass for all seven scripts |
| Complete project discovery | 108 run, 100 passed, 8 environment-gated skips |
| Secret scan | Pass; only the existing intentional `test-key` fixture matched |
| CloudFormation service validation | Blocked by authentication boundary |
| Live budget/SNS confirmation | Blocked by resource-creation approval boundary |
| Live S3/EMR/CloudWatch readiness | Blocked by resource-creation approval boundary |
| Live teardown inventory | Blocked until resources exist |

## IAM review

The EMR role can list only governed bucket prefixes, read only admitted application/Bronze inputs, write only Silver/Gold/quarantine/evidence/log outputs, use only the project KMS key, write only the project log group, and publish only the `ASTRAYAN/V1` metric namespace. It cannot delete S3 objects, access secrets or databases, mutate IAM, or pass roles.

`logs:DescribeLogGroups` and `cloudwatch:PutMetricData` require wildcard resources. The latter is namespace-conditioned. The KMS key policy uses `Resource: "*"` because it is the resource policy of that single key; principals and CloudWatch encryption context constrain it.

Database, ECS execution, loader, and verifier permissions remain absent because Phase 9B does not create those services. Premature grants would violate least privilege.

## Teardown safety

The foundation teardown records inventory, requires the exact confirmation phrase, requests CloudFormation deletion, waits for completion, and records remaining tagged resources. S3 buckets and the KMS key use retention protection. Cost controls have a separate teardown that refuses while the foundation exists, keeping alerts active until billing settles. Neither procedure can silently delete governed evidence or schedule the encryption key for deletion.

Live teardown acceptance requires zero running EMR applications/jobs, ECS tasks/services, RDS instances/restores, NAT gateways, load balancers, and unapproved interface endpoints. Retained evidence must be itemized with its ongoing cost.

## Required live closure steps

1. Receive explicit approval for authenticated AWS access and foundation resource creation.
2. Install or make available an approved AWS CLI without committing credentials.
3. Authenticate with an MFA-backed SSO/federated identity and record the account/region without exposing credentials.
4. Run AWS CloudFormation service-side validation and a change-set review with `CAPABILITY_NAMED_IAM`.
5. Confirm current pricing, account CloudTrail, billing metrics, EMR 8.0.0 availability, and the 16-vCPU quota.
6. Deploy the foundation; confirm the SNS subscription before any workload.
7. Verify budget, alarms, tags, policies, bucket controls, network, EMR readiness, logging, and zero job runs.
8. Capture pre-teardown inventory and accrued cost.
9. Teardown the stack, inventory retained protected resources, and prove no orphan hourly resources.
10. Update this report and required project documentation, rerun all tests, and commit the closed Phase 9B gate.

No 5M or 10M run is permitted during these steps.
