param(
    [string]$StackName = "astrayan-v1-cost-controls",
    [string]$Region = "us-east-1",
    [Parameter(Mandatory = $true)][ValidateSet("DELETE-ASTRAYAN-COST-CONTROLS")][string]$Confirmation
)

$ErrorActionPreference = "Stop"
if ($Region -ne "us-east-1") { throw "Cost controls are region-locked to us-east-1." }
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) { throw "AWS CLI is required." }

$foundation = aws cloudformation describe-stacks --stack-name astrayan-v1-foundation --region $Region --query "Stacks[0].StackStatus" --output text 2>$null
if ($LASTEXITCODE -eq 0 -and $foundation) { throw "Delete the foundation stack and verify final billing before removing cost controls." }
aws cloudformation delete-stack --stack-name $StackName --region $Region
if ($LASTEXITCODE -ne 0) { throw "Cost-control stack delete request failed." }
aws cloudformation wait stack-delete-complete --stack-name $StackName --region $Region
if ($LASTEXITCODE -ne 0) { throw "Cost-control stack deletion did not complete." }
Write-Output "Cost-control stack deleted after foundation absence was verified."
