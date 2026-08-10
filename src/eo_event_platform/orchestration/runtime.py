"""Deterministic operational metadata used by the Phase 7 Airflow DAG."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

CONTRACT_VERSION = "phase7-airflow-v1"
DAG_ID = "nasa_eo_batch_vertical_slice_v1"
PROCESSING_STAGES = (
    "nasa_extraction",
    "canonical_transformation",
    "controlled_replay",
    "spark_processing",
    "gold_generation",
    "postgres_load",
    "verification",
)
MAX_GATE_SIZE = 1_000_000
MAX_INTEGRATION_GATE_SIZE = 100


class StageFailure(RuntimeError):
    """Raised after a stage failure has been recorded."""


@dataclass(frozen=True)
class RunParameters:
    gate_size: int
    source_detection_count: int
    replay_factor: int
    execution_profile: str
    source_date: str

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "RunParameters":
        params = cls(
            gate_size=int(values["gate_size"]),
            source_detection_count=int(values["source_detection_count"]),
            replay_factor=int(values["replay_factor"]),
            execution_profile=str(values["execution_profile"]),
            source_date=str(values["source_date"]),
        )
        params.validate()
        return params

    def validate(self) -> None:
        if not 1 <= self.gate_size <= MAX_GATE_SIZE:
            raise ValueError("gate_size must be between 1 and 1,000,000")
        if not 1 <= self.source_detection_count <= 10_000:
            raise ValueError("source_detection_count must be between 1 and 10,000")
        if not 1 <= self.replay_factor <= 100:
            raise ValueError("replay_factor must be between 1 and 100")
        if self.source_detection_count * self.replay_factor != self.gate_size:
            raise ValueError("source_detection_count * replay_factor must equal gate_size")
        if self.execution_profile not in {"local", "integration"}:
            raise ValueError("execution_profile must be local or integration")
        if self.execution_profile == "integration" and self.gate_size > MAX_INTEGRATION_GATE_SIZE:
            raise ValueError("integration profile is limited to 100 records")
        date.fromisoformat(self.source_date)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def build_run_identity(*, logical_date: str, params: RunParameters) -> str:
    params.validate()
    return _sha256(
        {
            "contract_version": CONTRACT_VERSION,
            "dag_id": DAG_ID,
            "logical_date": logical_date,
            "parameters": asdict(params),
        }
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    temporary.write_text(_canonical_json(value) + "\n", encoding="utf-8")
    temporary.replace(path)


def _manifest_path(metadata_root: Path, orchestration_run_id: str) -> Path:
    return metadata_root / f"orchestration_run_id={orchestration_run_id}" / "manifest.json"


def initialize_run(
    *,
    metadata_root: Path,
    airflow_run_id: str,
    logical_date: str,
    params: RunParameters,
    pipeline_revision: str,
) -> dict[str, Any]:
    orchestration_run_id = build_run_identity(logical_date=logical_date, params=params)
    path = _manifest_path(metadata_root, orchestration_run_id)
    identity = {
        "contract_version": CONTRACT_VERSION,
        "orchestration_run_id": orchestration_run_id,
        "dag_id": DAG_ID,
        "logical_date": logical_date,
        "parameters": asdict(params),
        "pipeline_revision": pipeline_revision,
    }
    if path.exists():
        existing = json.loads(path.read_text(encoding="utf-8"))
        for key, expected in identity.items():
            if existing.get(key) != expected:
                raise RuntimeError(f"immutable run identity conflict for {key}")
        if airflow_run_id not in existing["airflow_run_ids"]:
            existing["airflow_run_ids"].append(airflow_run_id)
            existing.setdefault("rerun_events", []).append(
                {
                    "airflow_run_id": airflow_run_id,
                    "recorded_at": _utc_now(),
                    "idempotent_reuse": True,
                }
            )
            existing["updated_at"] = _utc_now()
            _atomic_json(path, existing)
        return {**existing, "manifest_path": path.as_posix(), "idempotent_reuse": True}

    manifest = {
        **identity,
        "airflow_run_ids": [airflow_run_id],
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
        "status": "RUNNING",
        "stage_order": list(PROCESSING_STAGES),
        "stages": {},
    }
    _atomic_json(path, manifest)
    return {**manifest, "manifest_path": path.as_posix(), "idempotent_reuse": False}


def _load_context(context: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    path = Path(str(context["manifest_path"]))
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest["orchestration_run_id"] != context["orchestration_run_id"]:
        raise RuntimeError("operational metadata identity mismatch")
    return path, manifest


def _integration_output(stage: str, context: Mapping[str, Any], upstream_checksum: str) -> dict[str, Any]:
    params = context["parameters"]
    record_count = int(params["gate_size"])
    return {
        "profile": "integration",
        "stage": stage,
        "record_count": record_count,
        "upstream_checksum": upstream_checksum,
        "artifact_checksum": _sha256(
            {
                "orchestration_run_id": context["orchestration_run_id"],
                "stage": stage,
                "record_count": record_count,
                "upstream_checksum": upstream_checksum,
            }
        ),
    }


def _local_output(stage: str, context: Mapping[str, Any], upstream_checksum: str) -> dict[str, Any]:
    """Run the repository-approved command for a stage without invoking a shell.

    Commands are injected as JSON arrays through stage-specific environment variables.
    This keeps credentials out of DAG parameters/XCom and preserves the proven Phase 6
    CLIs and Docker invocations without duplicating their processing behavior.
    """
    variable = f"ASTRAYAN_{stage.upper()}_COMMAND"
    raw = os.environ.get(variable)
    if raw is None:
        raise RuntimeError(f"required local stage command is not configured: {variable}")
    command = json.loads(raw)
    if not isinstance(command, list) or not command or not all(isinstance(item, str) for item in command):
        raise ValueError(f"{variable} must be a non-empty JSON string array")
    completed = subprocess.run(command, check=True, capture_output=True, text=True, timeout=None)
    output = {
        line.split("=", 1)[0]: line.split("=", 1)[1]
        for line in completed.stdout.splitlines()
        if "=" in line
    }
    return {
        "profile": "local",
        "stage": stage,
        "upstream_checksum": upstream_checksum,
        "command_fingerprint": _sha256(command),
        "output": output,
    }


def execute_stage(
    *,
    context: Mapping[str, Any],
    stage: str,
    upstream: Mapping[str, Any] | None = None,
    fail_for_test: bool = False,
) -> dict[str, Any]:
    if stage not in PROCESSING_STAGES:
        raise ValueError(f"unknown stage: {stage}")
    path, manifest = _load_context(context)
    position = PROCESSING_STAGES.index(stage)
    if position:
        prior_stage = PROCESSING_STAGES[position - 1]
        prior = manifest["stages"].get(prior_stage)
        if not prior or prior["status"] != "SUCCEEDED":
            raise StageFailure(f"upstream stage is not successful: {prior_stage}")
    upstream_checksum = "ROOT" if upstream is None else str(upstream["output_checksum"])
    existing = manifest["stages"].get(stage)
    if existing and existing["status"] == "SUCCEEDED":
        if existing["upstream_checksum"] != upstream_checksum:
            raise RuntimeError(f"successful stage conflict: {stage}")
        return {**existing, "idempotent_reuse": True}

    attempt = 1 if existing is None else int(existing["attempt"]) + 1
    started_at = _utc_now()
    started = time.perf_counter()
    try:
        if fail_for_test:
            raise RuntimeError("controlled Phase 7 failure")
        profile = context["parameters"]["execution_profile"]
        output = (
            _integration_output(stage, context, upstream_checksum)
            if profile == "integration"
            else _local_output(stage, context, upstream_checksum)
        )
        output_checksum = _sha256(output)
        receipt = {
            "stage": stage,
            "status": "SUCCEEDED",
            "attempt": attempt,
            "started_at": started_at,
            "completed_at": _utc_now(),
            "duration_seconds": round(time.perf_counter() - started, 6),
            "upstream_checksum": upstream_checksum,
            "output_checksum": output_checksum,
            "output": output,
        }
    except Exception as exc:
        receipt = {
            "stage": stage,
            "status": "FAILED",
            "attempt": attempt,
            "started_at": started_at,
            "completed_at": _utc_now(),
            "duration_seconds": round(time.perf_counter() - started, 6),
            "upstream_checksum": upstream_checksum,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        manifest["stages"][stage] = receipt
        manifest["status"] = "FAILED"
        manifest["updated_at"] = _utc_now()
        _atomic_json(path, manifest)
        raise StageFailure(f"{stage} failed") from exc

    manifest["stages"][stage] = receipt
    manifest["status"] = "RUNNING"
    manifest["updated_at"] = _utc_now()
    _atomic_json(path, manifest)
    return {**receipt, "idempotent_reuse": False}


def finalize_run(*, context: Mapping[str, Any], verification: Mapping[str, Any]) -> dict[str, Any]:
    path, manifest = _load_context(context)
    if verification.get("stage") != "verification" or verification.get("status") != "SUCCEEDED":
        raise StageFailure("verification did not succeed")
    if any(manifest["stages"].get(stage, {}).get("status") != "SUCCEEDED" for stage in PROCESSING_STAGES):
        raise StageFailure("not all processing stages succeeded")
    manifest["status"] = "SUCCEEDED"
    manifest["completed_at"] = _utc_now()
    manifest["updated_at"] = _utc_now()
    manifest["reconciliation"] = {
        "expected_stage_count": len(PROCESSING_STAGES),
        "successful_stage_count": len(PROCESSING_STAGES),
        "record_count": manifest["parameters"]["gate_size"],
        "verified": True,
    }
    _atomic_json(path, manifest)
    return {**manifest, "manifest_path": path.as_posix()}
