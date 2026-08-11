param(
    [Parameter(Mandatory = $true)][string]$NotificationEmail,
    [Parameter(Mandatory = $true)][ValidatePattern("^20[0-9]{2}-(0[1-9]|1[0-2])-([0-2][0-9]|3[0-1])$")][string]$ExpiresAt,
    [string]$OwnerTag = "Nitheesh2325",
    [string]$StackName = "astrayan-v1-cost-controls",
    [string]$Region = "us-east-1",
    [Parameter(Mandatory = $true)][ValidateSet("CREATE-ASTRAYAN-COST-CONTROLS")][string]$Confirmation
)

$ErrorActionPreference = "Stop"
if ($Region -ne "us-east-1") { throw "Cost controls are region-locked to us-east-1." }
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) { throw "AWS CLI is required." }
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$templatePath = Join-Path (Split-Path -Parent $scriptRoot) "cloudformation/cost-controls.json"

aws cloudformation validate-template --region $Region --template-body "file://$templatePath"
if ($LASTEXITCODE -ne 0) { throw "Cost-control template validation failed." }
aws cloudformation deploy --stack-name $StackName --region $Region --template-file $templatePath --no-fail-on-empty-changeset --parameter-overrides NotificationEmail=$NotificationEmail OwnerTag=$OwnerTag ExpiresAt=$ExpiresAt MonthlyBudgetUsd=50 --tags Project=ASTRAYAN Environment=portfolio-v1 Owner=$OwnerTag ManagedBy=CloudFormation CostCenter=portfolio Gate=cost-controls ExpiresAt=$ExpiresAt
if ($LASTEXITCODE -ne 0) { throw "Cost-control deployment failed." }
Write-Output "Cost controls created. Confirm the SNS email, then run verify-cost-controls.ps1. Do not deploy the foundation before verification passes."
