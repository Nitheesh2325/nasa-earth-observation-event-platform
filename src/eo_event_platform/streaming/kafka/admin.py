"""Create and verify the explicit local Kafka topic contract."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from confluent_kafka.admin import AdminClient, NewTopic

from .contracts import TOPIC_CONTRACTS


def create_topics(bootstrap_servers: str) -> list[dict[str, object]]:
    """Idempotently create governed topics and verify partition metadata."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers, "client.id": "eo-topic-admin"})
    metadata = admin.list_topics(timeout=15)
    existing = set(metadata.topics)
    missing_contracts = [contract for contract in TOPIC_CONTRACTS if contract.name not in existing]
    if missing_contracts:
        futures = admin.create_topics(
            [
                NewTopic(
                    contract.name,
                    num_partitions=contract.partitions,
                    replication_factor=contract.replication_factor,
                    config=contract.config,
                )
                for contract in missing_contracts
            ],
            operation_timeout=15,
        )
        for topic_name, future in futures.items():
            try:
                future.result(20)
            except Exception as exc:
                raise RuntimeError(f"topic creation failed for {topic_name}: {exc}") from exc

    verified = admin.list_topics(timeout=15)
    results: list[dict[str, object]] = []
    for contract in TOPIC_CONTRACTS:
        topic = verified.topics.get(contract.name)
        if topic is None or topic.error is not None:
            raise RuntimeError(f"topic metadata unavailable: {contract.name}")
        actual_partitions = len(topic.partitions)
        if actual_partitions != contract.partitions:
            raise RuntimeError(
                f"topic {contract.name} has {actual_partitions} partitions; expected {contract.partitions}"
            )
        results.append({**asdict(contract), "config": contract.config})
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args()
    for result in create_topics(args.bootstrap_servers):
        print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

