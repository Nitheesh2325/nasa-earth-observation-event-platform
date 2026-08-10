"""Explicit Phase 8A request and response contracts."""

from __future__ import annotations

import base64
import binascii
import json
from datetime import date, datetime, timedelta
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

SourceType = Literal["NASA_ORIGINAL", "NASA_REPLAY", "SYNTHETIC_SCALE_TEST"]
Longitude = Annotated[float, Field(ge=-180, le=180)]
Latitude = Annotated[float, Field(ge=-90, le=90)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ActivityRange(StrictModel):
    start_time: datetime | None = None
    end_time: datetime | None = None

    @model_validator(mode="after")
    def validate_range(self):
        if (self.start_time is None) != (self.end_time is None):
            raise ValueError("start_time and end_time must be provided together")
        if self.start_time is not None:
            if self.start_time.tzinfo is None or self.end_time.tzinfo is None:
                raise ValueError("activity timestamps must include a timezone")
            if self.start_time >= self.end_time:
                raise ValueError("start_time must be before end_time")
            if self.end_time - self.start_time > timedelta(days=31):
                raise ValueError("activity range cannot exceed 31 days")
        return self


class SummaryQuery(ActivityRange):
    source_type: SourceType | None = None
    source_dataset: str | None = Field(default=None, min_length=1, max_length=100)


class DailyQuery(StrictModel):
    start_date: date
    end_date: date
    source_type: SourceType | None = None
    source_dataset: str | None = Field(default=None, min_length=1, max_length=100)
    limit: int = Field(default=100, ge=1, le=200)

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be after end_date")
        if self.end_date - self.start_date > timedelta(days=30):
            raise ValueError("daily range cannot exceed 31 inclusive days")
        return self


class PageQuery(StrictModel):
    limit: int = Field(default=50, ge=1, le=100)
    cursor: str | None = Field(default=None, min_length=1, max_length=1024)


class BoundingBoxQuery(StrictModel):
    min_longitude: Longitude
    min_latitude: Latitude
    max_longitude: Longitude
    max_latitude: Latitude
    start_time: datetime
    end_time: datetime
    source_type: SourceType | None = None
    source_dataset: str | None = Field(default=None, min_length=1, max_length=100)
    limit: int = Field(default=100, ge=1, le=500)
    cursor: str | None = Field(default=None, min_length=1, max_length=1024)

    @model_validator(mode="after")
    def validate_bounds(self):
        if self.min_longitude >= self.max_longitude:
            raise ValueError("min_longitude must be less than max_longitude")
        if self.min_latitude >= self.max_latitude:
            raise ValueError("min_latitude must be less than max_latitude")
        if self.start_time.tzinfo is None or self.end_time.tzinfo is None:
            raise ValueError("activity timestamps must include a timezone")
        if self.start_time >= self.end_time:
            raise ValueError("start_time must be before end_time")
        if self.end_time - self.start_time > timedelta(days=7):
            raise ValueError("bounding-box activity range cannot exceed 7 days")
        return self


class ReadinessResponse(StrictModel):
    status: Literal["ready"]
    database: Literal["reachable"]
    database_role: str
    read_only: bool


class PlatformSummaryResponse(StrictModel):
    time_semantics: Literal["activity_time"] = "activity_time"
    event_message_count: int
    unique_event_count: int
    unique_detection_count: int
    original_message_count: int
    replay_message_count: int
    synthetic_message_count: int
    first_observation_time: datetime | None
    last_observation_time: datetime | None
    first_activity_time: datetime | None
    last_activity_time: datetime | None


class DailyAggregateItem(StrictModel):
    activity_date: date
    source_dataset: str
    source_type: SourceType
    is_synthetic: bool
    event_message_count: int
    unique_event_count: int
    unique_detection_count: int


class DailyAggregateResponse(StrictModel):
    time_semantics: Literal["activity_time"] = "activity_time"
    items: list[DailyAggregateItem]


class EventItem(StrictModel):
    event_id: str
    detection_id: str
    lineage_root_id: str
    source_type: SourceType
    source_dataset: str
    source_record_id: str
    is_synthetic: bool
    event_timestamp: datetime
    activity_timestamp: datetime
    scheduled_replay_timestamp: datetime | None
    replay_iteration: int | None
    replay_sequence_number: int | None
    parent_event_id: str | None
    latitude: float
    longitude: float


class LineageSummary(StrictModel):
    lineage_root_id: str
    event_message_count: int
    unique_event_count: int
    unique_detection_count: int
    original_message_count: int
    replay_message_count: int
    synthetic_message_count: int
    first_observation_time: datetime
    last_activity_time: datetime


class LineageResponse(StrictModel):
    time_semantics: Literal["activity_time"] = "activity_time"
    summary: LineageSummary
    events: list[EventItem]
    next_cursor: str | None


class GeoJSONGeometry(StrictModel):
    type: Literal["Point"] = "Point"
    coordinates: tuple[float, float]


class GeoJSONFeature(StrictModel):
    type: Literal["Feature"] = "Feature"
    id: str
    geometry: GeoJSONGeometry
    properties: EventItem


class GeoJSONFeatureCollection(StrictModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    time_semantics: Literal["activity_time"] = "activity_time"
    features: list[GeoJSONFeature]
    next_cursor: str | None


def encode_cursor(activity_timestamp: datetime, event_id: str) -> str:
    payload = json.dumps(
        {"activity_timestamp": activity_timestamp.isoformat(), "event_id": event_id},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_cursor(value: str | None) -> tuple[datetime, str] | None:
    if value is None:
        return None
    try:
        padded = value + "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
        if set(payload) != {"activity_timestamp", "event_id"}:
            raise ValueError
        timestamp = datetime.fromisoformat(payload["activity_timestamp"])
        event_id = payload["event_id"]
        if timestamp.tzinfo is None or not isinstance(event_id, str) or not event_id:
            raise ValueError
        return timestamp, event_id
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError("invalid pagination cursor") from exc
