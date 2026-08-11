param(
    [switch]$Online,
    [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Stop"
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$awsRoot = Split-Path -Parent $scriptRoot
$templatePath = Join-Path $awsRoot "cloudformation/foundation.json"
$costTemplatePath = Join-Path $awsRoot "cloudformation/cost-controls.json"

if ($Region -ne "us-east-1") {
    throw "Phase 9B is region-locked to us-east-1."
}

$template = Get-Content -Raw -LiteralPath $templatePath | ConvertFrom-Json
$costTemplate = Get-Content -Raw -LiteralPath $costTemplatePath | ConvertFrom-Json
if ($template.AWSTemplateFormatVersion -ne "2010-09-09") {
    throw "Unexpected CloudFormation template version."
}

$required = @(
    "DataKey", "DataBucket", "LogBucket", "EmrRuntimeRole", "EmrApplication",
    "EmrLogGroup", "EmrFailureAlarm", "Vpc", "S3Endpoint"
)
foreach ($logicalId in $required) {
    if (-not $template.Resources.PSObject.Properties.Name.Contains($logicalId)) {
        throw "Missing required resource: $logicalId"
    }
}

foreach ($logicalId in @("AlertTopic", "MonthlyCostBudget", "BillingAlarm25", "BillingAlarm40")) {
    if (-not $costTemplate.Resources.PSObject.Properties.Name.Contains($logicalId)) {
        throw "Missing required cost-control resource: $logicalId"
    }
}

if ($template.Resources.EmrApplication.Properties.MaximumCapacity.Cpu -ne "16 vCPU") {
    throw "EMR maximum CPU must remain 16 vCPU for Phase 9B."
}
if ($template.Resources.EmrApplication.Properties.AutoStopConfiguration.IdleTimeoutMinutes -ne 10) {
    throw "EMR idle timeout must remain 10 minutes."
}

Write-Output "Local CloudFormation structural validation: PASS"

if ($Online) {
    if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
        throw "AWS CLI is required for optional online validation."
    }
    aws cloudformation validate-template --region $Region --template-body "file://$templatePath"
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CloudFormation online validation failed."
    }
    aws cloudformation validate-template --region $Region --template-body "file://$costTemplatePath"
    if ($LASTEXITCODE -ne 0) {
        throw "AWS cost-control template online validation failed."
    }
    Write-Output "AWS CloudFormation online validation: PASS"
}
