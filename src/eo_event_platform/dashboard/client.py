"""Validated HTTP client for the six approved dashboard API routes."""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, ValidationError

from eo_event_platform.api.models import (
    DailyAggregateResponse,
    GeoJSONFeatureCollection,
    LineageResponse,
    PlatformStatusResponse,
    PlatformSummaryResponse,
    ReadinessResponse,
)

ResponseModel = TypeVar("ResponseModel", bound=BaseModel)
DEFAULT_API_BASE_URL = "http://127.0.0.1:8000"


class DashboardApiError(RuntimeError):
    """Safe dashboard-facing API boundary error."""


class DashboardApiClient:
    def __init__(
        self,
        base_url: str | None = None,
        *,
        timeout_seconds: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        resolved = (base_url or os.environ.get("EO_DASHBOARD_API_BASE_URL", DEFAULT_API_BASE_URL)).rstrip("/")
        if not resolved.startswith(("http://", "https://")):
            raise ValueError("dashboard API base URL must use HTTP or HTTPS")
        self._client = httpx.Client(base_url=resolved, timeout=timeout_seconds, transport=transport)

    def _get(self, path: str, model: type[ResponseModel], params: dict[str, Any] | None = None) -> ResponseModel:
        try:
            response = self._client.get(path, params=params)
            response.raise_for_status()
            return model.model_validate(response.json())
        except (httpx.HTTPError, ValueError, ValidationError) as exc:
            raise DashboardApiError("FastAPI data is unavailable or invalid") from exc

    def readiness(self) -> ReadinessResponse:
        return self._get("/health/ready", ReadinessResponse)

    def status(self) -> PlatformStatusResponse:
        return self._get("/v1/platform/status", PlatformStatusResponse)

    def summary(self) -> PlatformSummaryResponse:
        return self._get("/v1/summary", PlatformSummaryResponse)

    def daily(self, start_date: date, end_date: date, source_type: str | None = None) -> DailyAggregateResponse:
        params: dict[str, Any] = {"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "limit": 200}
        if source_type:
            params["source_type"] = source_type
        return self._get("/v1/daily", DailyAggregateResponse, params)

    def bbox(
        self,
        *,
        min_longitude: float,
        min_latitude: float,
        max_longitude: float,
        max_latitude: float,
        start_time: datetime,
        end_time: datetime,
        source_type: str | None,
        limit: int,
    ) -> GeoJSONFeatureCollection:
        params: dict[str, Any] = {
            "min_longitude": min_longitude, "min_latitude": min_latitude,
            "max_longitude": max_longitude, "max_latitude": max_latitude,
            "start_time": start_time.isoformat(), "end_time": end_time.isoformat(), "limit": limit,
        }
        if source_type:
            params["source_type"] = source_type
        return self._get("/v1/events/bbox", GeoJSONFeatureCollection, params)

    def lineage(self, lineage_root_id: str, limit: int = 100) -> LineageResponse:
        if not lineage_root_id.strip() or len(lineage_root_id) > 256:
            raise ValueError("lineage ID must contain 1 to 256 characters")
        return self._get(f"/v1/lineages/{lineage_root_id.strip()}", LineageResponse, {"limit": limit})
