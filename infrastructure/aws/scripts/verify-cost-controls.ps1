param(
    [string]$StackName = "astrayan-v1-cost-controls",
    [string]$Region = "us-east-1"
)

$ErrorActionPreference = "Stop"
if ($Region -ne "us-east-1") { throw "Cost controls are region-locked to us-east-1." }
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) { throw "AWS CLI is required." }

$topicArn = aws cloudformation describe-stacks --stack-name $StackName --region $Region --query "Stacks[0].Outputs[?OutputKey=='AlertTopicArn'].OutputValue | [0]" --output text
if ($LASTEXITCODE -ne 0 -or -not $topicArn.StartsWith("arn:aws:sns:us-east-1:")) { throw "Cost-control stack or alert topic output is unavailable." }
$pending = aws sns list-subscriptions-by-topic --topic-arn $topicArn --region $Region --query "Subscriptions[?SubscriptionArn=='PendingConfirmation'] | length(@)" --output text
if ($LASTEXITCODE -ne 0 -or [int]$pending -ne 0) { throw "SNS subscription is not confirmed." }
$budgetLimit = aws budgets describe-budget --account-id ((aws sts get-caller-identity --query Account --output text)) --budget-name astrayan-v1-monthly-cost --query "Budget.BudgetLimit.Amount" --output text
if ($LASTEXITCODE -ne 0 -or [decimal]$budgetLimit -ne 50) { throw "The required 50 USD budget is not active." }
$alarmCount = aws cloudwatch describe-alarms --region $Region --alarm-names astrayan-v1-estimated-charge-25-usd astrayan-v1-estimated-charge-40-usd --query "length(MetricAlarms)" --output text
if ($LASTEXITCODE -ne 0 -or [int]$alarmCount -ne 2) { throw "Both billing alarms are not active." }
Write-Output "Budget, billing alarms, and confirmed SNS subscription: PASS"
