param(
    [string]$StackName = "astrayan-v1-foundation",
    [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Stop"
if ($Region -ne "us-east-1") { throw "Phase 9B is region-locked to us-east-1." }
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) { throw "AWS CLI is required." }

Write-Output "Identity"
aws sts get-caller-identity --region $Region
Write-Output "CloudFormation stack"
aws cloudformation describe-stacks --stack-name $StackName --region $Region
Write-Output "Cost-control stack"
aws cloudformation describe-stacks --stack-name astrayan-v1-cost-controls --region $Region
Write-Output "Project-tagged resources"
aws resourcegroupstaggingapi get-resources --region $Region --tag-filters Key=Project,Values=ASTRAYAN
Write-Output "Running EMR Serverless applications"
aws emr-serverless list-applications --region $Region --states STARTED STARTING STOPPING
Write-Output "RDS instances (must remain empty in Phase 9B)"
aws rds describe-db-instances --region $Region --query "DBInstances[?contains(DBInstanceIdentifier, 'astrayan')]"
Write-Output "ECS tasks (must remain empty in Phase 9B)"
aws ecs list-clusters --region $Region
Write-Output "NAT gateways (must remain empty)"
aws ec2 describe-nat-gateways --region $Region --filter Name=tag:Project,Values=ASTRAYAN
