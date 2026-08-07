from __future__ import annotations

import unittest
from inspect import Parameter, signature

from eo_event_platform.streaming.kafka.contracts import (
    REPLAY_TOPIC,
    TOPIC_CONTRACTS,
    diagnostic_consumer_config,
    producer_config,
)
from eo_event_platform.streaming.kafka.consumer import consume_producer_run


class KafkaContractTests(unittest.TestCase):
    def test_replay_topic_has_six_bounded_partitions(self) -> None:
        replay = next(contract for contract in TOPIC_CONTRACTS if contract.name == REPLAY_TOPIC)
        self.assertEqual(6, replay.partitions)
        self.assertEqual(1, replay.replication_factor)
        self.assertEqual(str(128 * 1024 * 1024), replay.config["retention.bytes"])
        self.assertEqual("delete", replay.config["cleanup.policy"])

    def test_producer_is_idempotent_and_bounded(self) -> None:
        config = producer_config("localhost:9092", "test-client")
        self.assertTrue(config["enable.idempotence"])
        self.assertEqual("all", config["acks"])
        self.assertEqual(5, config["message.send.max.retries"])
        self.assertEqual("zstd", config["compression.type"])
        self.assertLessEqual(config["max.in.flight.requests.per.connection"], 5)

    def test_diagnostic_consumer_never_auto_commits(self) -> None:
        config = diagnostic_consumer_config("localhost:9092", "test-group")
        self.assertFalse(config["enable.auto.commit"])
        self.assertFalse(config["enable.auto.offset.store"])

    def test_diagnostic_consumer_requires_truth_expectations(self) -> None:
        parameters = signature(consume_producer_run).parameters
        self.assertIs(Parameter.empty, parameters["expected_source_type"].default)
        self.assertIs(Parameter.empty, parameters["expected_detection_count"].default)
        self.assertIs(Parameter.empty, parameters["expected_replay_factor"].default)


if __name__ == "__main__":
    unittest.main()
