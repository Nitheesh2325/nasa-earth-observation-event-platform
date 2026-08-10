"""Version 1.0 read-only FastAPI application."""

from __future__ import annotations

import os
from typing import Annotated, Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse

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


def get_repository(request: Request) -> ApiRepository:
    return request.app.state.repository


def create_app(repository: ApiRepository | None = None) -> FastAPI:
    application = FastAPI(
        title="ASTRAYAN Earth Observation Event API",
        version="1.0.0",
        description="Read-only serving API with explicit NASA original, replay, and synthetic truth.",
    )
    application.state.repository = repository or ApiRepository(os.environ.get("EO_API_DATABASE_DSN", ""))

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
        filters: Annotated[SummaryQuery, Query()],
        repo: Annotated[ApiRepository, Depends(get_repository)],
    ) -> Any:
        return repo.summary(filters)

    @application.get("/v1/daily", response_model=DailyAggregateResponse, tags=["serving"])
    def daily(
        filters: Annotated[DailyQuery, Query()],
        repo: Annotated[ApiRepository, Depends(get_repository)],
    ) -> Any:
        return {"items": repo.daily(filters)}

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
