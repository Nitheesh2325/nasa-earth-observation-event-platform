"""Versioned deterministic identities for controlled NASA replay."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass


REPLAY_IDENTITY_ALGORITHM = "nasa-replay-v1"
REPLAY_ORDERING_ALGORITHM = "replay-iteration-then-event-id-v1"


@dataclass(frozen=True)
class ReplayPlan:
    """Identity-bearing logical parameters for one replay plan."""

    source_input_sha256: str
    source_record_count: int
    replay_factor: int
    scheduled_replay_start: str
    scheduled_interval_milliseconds: int
    identity_algorithm: str = REPLAY_IDENTITY_ALGORITHM
    ordering_algorithm: str = REPLAY_ORDERING_ALGORITHM

    def validate(self) -> None:
        if len(self.source_input_sha256) != 64:
            raise ValueError("source_input_sha256 must be a SHA-256 hex digest")
        try:
            int(self.source_input_sha256, 16)
        except ValueError as exc:
            raise ValueError("source_input_sha256 must be hexadecimal") from exc
        if self.source_record_count <= 0:
            raise ValueError("source_record_count must be positive")
        if self.replay_factor <= 0:
            raise ValueError("replay_factor must be positive")
        if self.scheduled_interval_milliseconds <= 0:
            raise ValueError("scheduled_interval_milliseconds must be positive")


def canonical_json_bytes(value: dict[str, object]) -> bytes:
    """Serialize identity material with stable key and separator rules."""
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def build_replay_run_id(plan: ReplayPlan) -> str:
    """Return the stable logical replay-plan identity."""
    plan.validate()
    digest = hashlib.sha256(canonical_json_bytes(asdict(plan))).hexdigest()
    return f"{REPLAY_IDENTITY_ALGORITHM}:sha256:{digest}"


def build_replay_event_id(
    *, replay_run_id: str, parent_event_id: str, replay_iteration: int
) -> str:
    """Return a stable identity for one replay event message."""
    if not replay_run_id.startswith(f"{REPLAY_IDENTITY_ALGORITHM}:sha256:"):
        raise ValueError("replay_run_id does not use nasa-replay-v1")
    if not parent_event_id:
        raise ValueError("parent_event_id is required")
    if replay_iteration <= 0:
        raise ValueError("replay_iteration must be positive")
    material = {
        "identity_algorithm": REPLAY_IDENTITY_ALGORITHM,
        "parent_event_id": parent_event_id,
        "replay_iteration": replay_iteration,
        "replay_run_id": replay_run_id,
    }
    digest = hashlib.sha256(canonical_json_bytes(material)).hexdigest()
    return f"{REPLAY_IDENTITY_ALGORITHM}:sha256:{digest}"

