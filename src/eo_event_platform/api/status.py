"""Bounded composition of existing operational metadata for the read-only API."""

from __future__ import annotations

import json
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Any

from .cache import CacheBackend
from .repository import ApiRepository

MAX_OPERATIONAL_MANIFESTS = 1_000


class OperationalMetadataReader:
    def __init__(self, root: Path | None):
        self._root = root

    def latest(self) -> dict[str, Any]:
        if self._root is None or not self._root.is_dir():
            raise RuntimeError("operational metadata is unavailable")
        paths = list(islice(self._root.glob("orchestration_run_id=*/manifest.json"), MAX_OPERATIONAL_MANIFESTS + 1))
        if not paths or len(paths) > MAX_OPERATIONAL_MANIFESTS:
            raise RuntimeError("operational metadata is unavailable or exceeds its bound")
        manifests = [self._validated(path) for path in paths]
        latest = max(manifests, key=lambda value: datetime.fromisoformat(value["updated_at"]))
        successful = [value for value in manifests if value["status"] == "SUCCEEDED"]
        if not successful:
            raise RuntimeError("no successful pipeline run is available")
        last_success = max(successful, key=lambda value: datetime.fromisoformat(value["completed_at"]))
        return {
            "last_successful_pipeline_run": last_success["completed_at"],
            "latest_airflow_run_id": latest["airflow_run_ids"][-1],
            "latest_airflow_status": latest["status"],
        }

    @staticmethod
    def _validated(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            required = {"status", "updated_at", "airflow_run_ids"}
            if not required.issubset(value) or value["status"] not in {"SUCCEEDED", "FAILED", "RUNNING"}:
                raise ValueError
            if not isinstance(value["airflow_run_ids"], list) or not value["airflow_run_ids"]:
                raise ValueError
            datetime.fromisoformat(value["updated_at"])
            if value["status"] == "SUCCEEDED":
                datetime.fromisoformat(value["completed_at"])
            return value
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("invalid operational metadata") from exc


class PlatformStatusService:
    def __init__(
        self,
        repository: ApiRepository,
        metadata_reader: OperationalMetadataReader,
        cache_backend: CacheBackend,
        cache_ttl_seconds: float,
        api_version: str,
    ) -> None:
        self._repository = repository
        self._metadata_reader = metadata_reader
        self._cache_backend = cache_backend
        self._cache_ttl_seconds = cache_ttl_seconds
        self._api_version = api_version

    def status(self) -> dict[str, Any]:
        database = self._repository.platform_status()
        airflow = self._metadata_reader.latest()
        snapshot_method = getattr(self._cache_backend, "snapshot", None)
        snapshot = snapshot_method() if callable(snapshot_method) else None
        return {
            **airflow,
            "latest_manifest_id": str(database["latest_manifest_id"]),
            "latest_manifest_sha256": database["latest_manifest_sha256"],
            "latest_gold_version": database["latest_gold_version"],
            "cache_enabled": self._cache_backend is not None,
            "cache_ttl_seconds": self._cache_ttl_seconds,
            "cache_entries": snapshot.entries if snapshot is not None else None,
            "api_version": self._api_version,
            "platform_version": database["platform_version"],
            "data_freshness": database["data_freshness"],
            "quality_gate_status": database["quality_gate_status"],
        }
