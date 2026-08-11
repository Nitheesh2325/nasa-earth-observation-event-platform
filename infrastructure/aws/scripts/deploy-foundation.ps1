param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^20[0-9]{2}-(0[1-9]|1[0-2])-([0-2][0-9]|3[0-1])$")]
    [string]$ExpiresAt,
    [string]$OwnerTag = "Nitheesh2325",
    [string]$StackName = "astrayan-v1-foundation",
    [string]$Region = "us-east-1",
    [Parameter(Mandatory = $true)]
    [ValidateSet("CREATE-ASTRAYAN-FOUNDATION")]
    [string]$Confirmation
)

$ErrorActionPreference = "Stop"
if ($Region -ne "us-east-1") { throw "Phase 9B is region-locked to us-east-1." }
if ($Confirmation -ne "CREATE-ASTRAYAN-FOUNDATION") { throw "Exact creation confirmation is required." }
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) { throw "AWS CLI is required." }

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$awsRoot = Split-Path -Parent $scriptRoot
$templatePath = Join-Path $awsRoot "cloudformation/foundation.json"
$costStackName = "astrayan-v1-cost-controls"

& (Join-Path $scriptRoot "validate-foundation.ps1") -Online -Region $Region
aws sts get-caller-identity --region $Region
if ($LASTEXITCODE -ne 0) { throw "AWS identity validation failed." }

& (Join-Path $scriptRoot "verify-cost-controls.ps1") -StackName $costStackName -Region $Region
$alertTopicArn = aws cloudformation describe-stacks --stack-name $costStackName --region $Region --query "Stacks[0].Outputs[?OutputKey=='AlertTopicArn'].OutputValue | [0]" --output text
if ($LASTEXITCODE -ne 0 -or -not $alertTopicArn.StartsWith("arn:aws:sns:us-east-1:")) { throw "Verified alert topic output is unavailable." }

aws cloudformation deploy `
    --stack-name $StackName `
    --region $Region `
    --template-file $templatePath `
    --capabilities CAPABILITY_NAMED_IAM `
    --no-fail-on-empty-changeset `
    --parameter-overrides AlertTopicArn=$alertTopicArn OwnerTag=$OwnerTag ExpiresAt=$ExpiresAt EmrReleaseLabel=emr-8.0.0 `
    --tags Project=ASTRAYAN Environment=portfolio-v1 Owner=$OwnerTag ManagedBy=CloudFormation CostCenter=portfolio Gate=foundation ExpiresAt=$ExpiresAt
if ($LASTEXITCODE -ne 0) { throw "CloudFormation deployment failed." }

aws cloudformation describe-stacks --stack-name $StackName --region $Region
Write-Output "Foundation created. Confirm the SNS email subscription and verify every control before any job submission."
