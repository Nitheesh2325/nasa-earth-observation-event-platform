import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TEMPLATE_PATH = ROOT / "infrastructure" / "aws" / "cloudformation" / "foundation.json"
COST_TEMPLATE_PATH = ROOT / "infrastructure" / "aws" / "cloudformation" / "cost-controls.json"


class AwsFoundationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.template = json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))
        cls.resources = cls.template["Resources"]
        cls.cost_template = json.loads(COST_TEMPLATE_PATH.read_text(encoding="utf-8"))
        cls.cost_resources = cls.cost_template["Resources"]

    def test_region_budget_and_cost_alarms_are_fixed(self) -> None:
        assertion = self.cost_template["Rules"]["SingleApprovedRegion"]["Assertions"][0]
        self.assertEqual(assertion["Assert"]["Fn::Equals"][1], "us-east-1")
        budget = self.cost_resources["MonthlyCostBudget"]["Properties"]
        self.assertEqual(budget["Budget"]["BudgetLimit"]["Amount"], {"Ref": "MonthlyBudgetUsd"})
        self.assertNotIn("CostFilters", budget["Budget"])
        self.assertEqual(len(budget["NotificationsWithSubscribers"]), 5)
        self.assertEqual(self.cost_resources["BillingAlarm25"]["Properties"]["Threshold"], 25)
        self.assertEqual(self.cost_resources["BillingAlarm40"]["Properties"]["Threshold"], 40)
        self.assertEqual(self.resources["EmrFailureAlarm"]["Properties"]["AlarmActions"], [{"Ref": "AlertTopicArn"}])

    def test_buckets_are_private_versioned_encrypted_and_lifecycle_managed(self) -> None:
        for logical_id in ("DataBucket", "LogBucket"):
            props = self.resources[logical_id]["Properties"]
            self.assertEqual(props["VersioningConfiguration"]["Status"], "Enabled")
            self.assertEqual(
                props["BucketEncryption"]["ServerSideEncryptionConfiguration"][0]
                ["ServerSideEncryptionByDefault"]["SSEAlgorithm"],
                "aws:kms",
            )
            self.assertTrue(all(props["PublicAccessBlockConfiguration"].values()))
            self.assertGreaterEqual(len(props["LifecycleConfiguration"]["Rules"]), 2)
        for policy_id in ("DataBucketPolicy", "LogBucketPolicy"):
            statements = self.resources[policy_id]["Properties"]["PolicyDocument"]["Statement"]
            self.assertIn("DenyInsecureTransport", {item["Sid"] for item in statements})

    def test_emr_runtime_is_bounded_private_and_observable(self) -> None:
        props = self.resources["EmrApplication"]["Properties"]
        self.assertEqual(props["ReleaseLabel"], {"Ref": "EmrReleaseLabel"})
        self.assertEqual(props["MaximumCapacity"], {"Cpu": "16 vCPU", "Memory": "64 GB", "Disk": "200 GB"})
        self.assertNotIn("InitialCapacity", props)
        self.assertEqual(props["AutoStopConfiguration"], {"Enabled": True, "IdleTimeoutMinutes": 10})
        self.assertEqual(len(props["NetworkConfiguration"]["SubnetIds"]), 2)
        monitoring = props["MonitoringConfiguration"]
        self.assertTrue(monitoring["CloudWatchLoggingConfiguration"]["Enabled"])
        self.assertTrue(monitoring["ManagedPersistenceMonitoringConfiguration"]["Enabled"])
        self.assertIn("S3MonitoringConfiguration", monitoring)

    def test_network_has_no_public_or_nat_resources(self) -> None:
        resource_types = {resource["Type"] for resource in self.resources.values()}
        self.assertNotIn("AWS::EC2::NatGateway", resource_types)
        self.assertNotIn("AWS::EC2::InternetGateway", resource_types)
        self.assertNotIn("AWS::ElasticLoadBalancingV2::LoadBalancer", resource_types)
        self.assertFalse(self.resources["PrivateSubnetA"]["Properties"]["MapPublicIpOnLaunch"])
        self.assertFalse(self.resources["PrivateSubnetB"]["Properties"]["MapPublicIpOnLaunch"])

    def test_iam_wildcards_are_limited_and_conditioned(self) -> None:
        statements = self.resources["EmrRuntimeRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        wildcard_statements = [statement for statement in statements if statement.get("Resource") == "*"]
        self.assertEqual({item["Sid"] for item in wildcard_statements}, {"DescribeLogGroupsUnavoidable", "PublishProjectMetrics"})
        metric = next(item for item in wildcard_statements if item["Sid"] == "PublishProjectMetrics")
        self.assertEqual(metric["Condition"]["StringEquals"]["cloudwatch:namespace"], "ASTRAYAN/V1")
        actions = {action for statement in statements for action in statement["Action"]}
        self.assertNotIn("s3:DeleteObject", actions)
        self.assertNotIn("iam:PassRole", actions)

    def test_no_paid_workload_or_traffic_resources_are_defined(self) -> None:
        forbidden = {
            "AWS::RDS::DBInstance", "AWS::ECS::Service", "AWS::ECS::TaskDefinition",
            "AWS::MSK::Cluster", "AWS::EKS::Cluster", "AWS::EMRServerless::JobRun",
        }
        resource_types = {resource["Type"] for resource in self.resources.values()}
        self.assertFalse(forbidden.intersection(resource_types))

    def test_required_tags_exist_on_core_taggable_resources(self) -> None:
        required = {"Project", "Environment", "Owner", "ManagedBy", "CostCenter", "Gate", "ExpiresAt"}
        for logical_id in ("DataKey", "DataBucket", "LogBucket", "Vpc", "EmrRuntimeRole", "EmrApplication"):
            tags = self.resources[logical_id]["Properties"]["Tags"]
            self.assertTrue(required.issubset({tag["Key"] for tag in tags}), logical_id)
        cost_tags = self.cost_resources["AlertTopic"]["Properties"]["Tags"]
        self.assertTrue(required.issubset({tag["Key"] for tag in cost_tags}))

    def test_cost_controls_are_a_strict_deployment_precondition(self) -> None:
        self.assertNotIn("MonthlyCostBudget", self.resources)
        self.assertIn("AlertTopicArn", self.template["Parameters"])
        self.assertEqual(
            self.cost_template["Outputs"]["AlertTopicArn"]["Value"],
            {"Ref": "AlertTopic"},
        )


if __name__ == "__main__":
    unittest.main()
