"""Version 1.0 read-only FastAPI application."""

from __future__ import annotations

import json
import logging
import os
from typing import Annotated, Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse

from .cache import BoundedTTLCache, CacheBackend, DEFAULT_TTL_SECONDS, cache_bypassed, deterministic_cache_key
from .models import (
    BoundingBoxQuery,
    DailyAggregateResponse,
    DailyQuery,
    GeoJSONFeatureCollection,
    LineageResponse,
    PageQuery,
    PlatformSummaryResponse,
    ReadinessResponse,
    SummaryQuery,
)
from .repository import ApiRepository


LOGGER = logging.getLogger(__name__)


def get_repository(request: Request) -> ApiRepository:
    return request.app.state.repository


def create_app(
    repository: ApiRepository | None = None,
    cache_backend: CacheBackend | None = None,
    cache_ttl_seconds: float = DEFAULT_TTL_SECONDS,
) -> FastAPI:
    if cache_ttl_seconds <= 0:
        raise ValueError("cache TTL must be positive")
    application = FastAPI(
        title="ASTRAYAN Earth Observation Event API",
        version="1.0.0",
        description="Read-only serving API with explicit NASA original, replay, and synthetic truth.",
    )
    application.state.repository = repository or ApiRepository(os.environ.get("EO_API_DATABASE_DSN", ""))
    application.state.cache_backend = cache_backend if cache_backend is not None else BoundedTTLCache()
    application.state.cache_ttl_seconds = cache_ttl_seconds

    def cached_aggregate(request: Request, namespace: str, filters: Any, loader: Any, response_type: Any) -> Any:
        bypass = cache_bypassed(request.headers.get("cache-control"))
        key = deterministic_cache_key(namespace, filters)
        cache = request.app.state.cache_backend
        if not bypass:
            try:
                cached = cache.get(key)
                if cached is not None:
                    return response_type.model_validate_json(cached).model_dump(mode="json")
            except Exception:
                LOGGER.warning("aggregate cache read failed; using PostgreSQL", extra={"cache_namespace": namespace})
        result = response_type.model_validate(loader()).model_dump(mode="json")
        if not bypass:
            try:
                payload = json.dumps(result, sort_keys=True, separators=(",", ":")).encode("utf-8")
                cache.set(key, payload, request.app.state.cache_ttl_seconds)
            except Exception:
                LOGGER.warning("aggregate cache write failed", extra={"cache_namespace": namespace})
        return result

    @application.exception_handler(psycopg.errors.QueryCanceled)
    async def query_timeout_handler(_request: Request, _exc: Exception):
        return JSONResponse(status_code=504, content={"detail": "database query timed out"})

    @application.exception_handler(psycopg.OperationalError)
    async def unavailable_handler(_request: Request, _exc: Exception):
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})

    @application.exception_handler(psycopg.DatabaseError)
    async def database_error_handler(_request: Request, _exc: Exception):
        return JSONResponse(status_code=500, content={"detail": "database query failed"})

    @application.exception_handler(PermissionError)
    async def role_error_handler(_request: Request, _exc: Exception):
        return JSONResponse(status_code=503, content={"detail": "database role is not ready"})

    @application.get("/health/ready", response_model=ReadinessResponse, tags=["health"])
    def readiness(repo: Annotated[ApiRepository, Depends(get_repository)]) -> Any:
        try:
            return repo.readiness()
        except PermissionError as exc:
            raise HTTPException(status_code=503, detail="database role is not ready") from exc

    @application.get("/v1/summary", response_model=PlatformSummaryResponse, tags=["serving"])
    def platform_summary(
        request: Request,
        filters: Annotated[SummaryQuery, Query()],
        repo: Annotated[ApiRepository, Depends(get_repository)],
    ) -> Any:
        return cached_aggregate(request, "summary", filters, lambda: repo.summary(filters), PlatformSummaryResponse)

    @application.get("/v1/daily", response_model=DailyAggregateResponse, tags=["serving"])
    def daily(
        request: Request,
        filters: Annotated[DailyQuery, Query()],
        repo: Annotated[ApiRepository, Depends(get_repository)],
    ) -> Any:
        return cached_aggregate(
            request, "daily", filters, lambda: {"items": repo.daily(filters)}, DailyAggregateResponse
        )

    @application.get("/v1/lineages/{lineage_root_id}", response_model=LineageResponse, tags=["serving"])
    def lineage(
        lineage_root_id: Annotated[str, Path(min_length=1, max_length=256)],
        page: Annotated[PageQuery, Query()],
        repo: Annotated[ApiRepository, Depends(get_repository)],
    ) -> Any:
        try:
            result = repo.lineage(lineage_root_id, page)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if result is None:
            raise HTTPException(status_code=404, detail="lineage not found")
        return result

    @application.get("/v1/events/bbox", response_model=GeoJSONFeatureCollection, tags=["serving"])
    def bbox(
        filters: Annotated[BoundingBoxQuery, Query()],
        repo: Annotated[ApiRepository, Depends(get_repository)],
    ) -> Any:
        try:
            return repo.bbox(filters)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    return application


app = create_app()
