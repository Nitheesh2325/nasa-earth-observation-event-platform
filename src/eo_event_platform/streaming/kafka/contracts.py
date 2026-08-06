"""Versioned local Kafka topic and client configuration contracts."""

from __future__ import annotations

from dataclasses import dataclass


REPLAY_TOPIC = "eo.events.replay.v1"
REJECTED_TOPIC = "eo.events.rejected.v1"
DLQ_TOPIC = "eo.events.dlq.v1"


@dataclass(frozen=True)
class TopicContract:
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    retention_bytes: int
    segment_bytes: int

    @property
    def config(self) -> dict[str, str]:
        return {
            "cleanup.policy": "delete",
            "retention.ms": str(self.retention_ms),
            "retention.bytes": str(self.retention_bytes),
            "segment.bytes": str(self.segment_bytes),
            "max.message.bytes": str(2 * 1024 * 1024),
            "min.insync.replicas": "1",
        }


TOPIC_CONTRACTS = (
    TopicContract(REPLAY_TOPIC, 6, 1, 86_400_000, 128 * 1024 * 1024, 64 * 1024 * 1024),
    TopicContract(REJECTED_TOPIC, 3, 1, 604_800_000, 64 * 1024 * 1024, 32 * 1024 * 1024),
    TopicContract(DLQ_TOPIC, 3, 1, 604_800_000, 64 * 1024 * 1024, 32 * 1024 * 1024),
)


def producer_config(bootstrap_servers: str, client_id: str) -> dict[str, object]:
    """Return the bounded idempotent producer contract."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "message.send.max.retries": 5,
        "delivery.timeout.ms": 120_000,
        "request.timeout.ms": 30_000,
        "retry.backoff.ms": 500,
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "zstd",
        "linger.ms": 10,
        "batch.size": 65_536,
        "queue.buffering.max.messages": 20_000,
    }


def diagnostic_consumer_config(
    bootstrap_servers: str, group_id: str
) -> dict[str, object]:
    """Return the bounded manual-assignment diagnostic consumer contract."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "error",
        "session.timeout.ms": 10_000,
    }

