param(
    [string]$StackName = "astrayan-v1-foundation",
    [string]$Region = "us-east-1",
    [Parameter(Mandatory = $true)]
    [ValidateSet("DELETE-ASTRAYAN-FOUNDATION")]
    [string]$Confirmation
)

$ErrorActionPreference = "Stop"
if ($Region -ne "us-east-1") { throw "Phase 9B is region-locked to us-east-1." }
if ($Confirmation -ne "DELETE-ASTRAYAN-FOUNDATION") { throw "Exact teardown confirmation is required." }
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) { throw "AWS CLI is required." }

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
& (Join-Path $scriptRoot "inventory.ps1") -StackName $StackName -Region $Region

aws cloudformation delete-stack --stack-name $StackName --region $Region
if ($LASTEXITCODE -ne 0) { throw "CloudFormation delete request failed." }
aws cloudformation wait stack-delete-complete --stack-name $StackName --region $Region
if ($LASTEXITCODE -ne 0) { throw "CloudFormation stack deletion did not complete cleanly." }

Write-Output "Stack deletion complete. Retained S3 buckets and KMS key require separate evidence-approved deletion."
aws resourcegroupstaggingapi get-resources --region $Region --tag-filters Key=Project,Values=ASTRAYAN
