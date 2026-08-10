"""Reviewed, parameterized PostgreSQL queries for the read-only API."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

from .models import BoundingBoxQuery, DailyQuery, PageQuery, SummaryQuery, decode_cursor, encode_cursor

ACTIVITY_SQL = "COALESCE(scheduled_replay_timestamp, event_timestamp)"
EVENT_COLUMNS = f"""event_id, detection_id, lineage_root_id, source_type, source_dataset,
source_record_id, is_synthetic, event_timestamp,
{ACTIVITY_SQL} AS activity_timestamp, scheduled_replay_timestamp,
replay_iteration, replay_sequence_number, parent_event_id, latitude, longitude"""


class ApiRepository:
    def __init__(self, dsn: str):
        self._dsn = dsn

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection[Any]]:
        if not self._dsn.strip():
            raise psycopg.OperationalError("EO_API_DATABASE_DSN is required")
        with psycopg.connect(
            self._dsn,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=5,
            application_name="astrayan_fastapi",
        ) as connection:
            role = connection.execute(
                """SELECT current_user AS database_role,
                  current_setting('transaction_read_only') AS read_only"""
            ).fetchone()
            if (role["database_role"], role["read_only"]) != ("eo_api_runtime", "on"):
                raise PermissionError("database session is not the approved read-only API role")
            yield connection

    def readiness(self) -> dict[str, Any]:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT current_user AS database_role, current_setting('transaction_read_only') AS read_only"
            ).fetchone()
        read_only = row["read_only"] == "on"
        if row["database_role"] != "eo_api_runtime" or not read_only:
            raise PermissionError("database session is not the approved read-only API role")
        return {"status": "ready", "database": "reachable", "database_role": row["database_role"], "read_only": True}

    @staticmethod
    def _filters(query: SummaryQuery | DailyQuery | BoundingBoxQuery) -> tuple[list[str], list[Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if query.source_type is not None:
            conditions.append("source_type = %s")
            params.append(query.source_type)
        if query.source_dataset is not None:
            conditions.append("source_dataset = %s")
            params.append(query.source_dataset)
        return conditions, params

    def summary(self, query: SummaryQuery) -> dict[str, Any]:
        conditions, params = self._filters(query)
        if query.start_time is not None:
            conditions.append(f"{ACTIVITY_SQL} >= %s AND {ACTIVITY_SQL} < %s")
            params.extend((query.start_time, query.end_time))
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        if not conditions:
            statement = f"""WITH daily AS (
                SELECT coalesce(sum(event_message_count),0)::bigint AS event_message_count,
                  coalesce(sum(unique_event_count),0)::bigint AS unique_event_count,
                  coalesce(sum(original_message_count),0)::bigint AS original_message_count,
                  coalesce(sum(replay_message_count),0)::bigint AS replay_message_count,
                  coalesce(sum(synthetic_message_count),0)::bigint AS synthetic_message_count
                FROM serving.dataset_daily_summary
              ), lineage AS (
                SELECT count(*)::bigint AS unique_detection_count,
                  min(first_event_timestamp) AS first_observation_time,
                  max(first_event_timestamp) AS last_observation_time
                FROM serving.detection_lineage_summary
              ), activity AS (
                SELECT (SELECT {ACTIVITY_SQL} FROM serving.event_detail
                          ORDER BY {ACTIVITY_SQL}, event_id LIMIT 1) AS first_activity_time,
                       (SELECT {ACTIVITY_SQL} FROM serving.event_detail
                          ORDER BY {ACTIVITY_SQL} DESC, event_id DESC LIMIT 1) AS last_activity_time
              ) SELECT * FROM daily CROSS JOIN lineage CROSS JOIN activity"""
            with self.connection() as connection:
                return connection.execute(statement).fetchone()

        statement = f"""SELECT count(*) AS event_message_count,
          count(DISTINCT event_id) AS unique_event_count,
          count(DISTINCT detection_id) AS unique_detection_count,
          count(*) FILTER (WHERE source_type='NASA_ORIGINAL') AS original_message_count,
          count(*) FILTER (WHERE source_type='NASA_REPLAY') AS replay_message_count,
          count(*) FILTER (WHERE source_type='SYNTHETIC_SCALE_TEST') AS synthetic_message_count,
          min(event_timestamp) AS first_observation_time,
          max(event_timestamp) AS last_observation_time,
          min({ACTIVITY_SQL}) AS first_activity_time,
          max({ACTIVITY_SQL}) AS last_activity_time
          FROM serving.event_detail{where}"""
        with self.connection() as connection:
            return connection.execute(statement, tuple(params)).fetchone()

    def daily(self, query: DailyQuery) -> list[dict[str, Any]]:
        conditions = ["activity_date >= %s", "activity_date <= %s"]
        params: list[Any] = [query.start_date, query.end_date]
        if query.source_type is not None:
            conditions.append("source_type = %s")
            params.append(query.source_type)
        if query.source_dataset is not None:
            conditions.append("source_dataset = %s")
            params.append(query.source_dataset)
        statement = f"""SELECT activity_date, source_dataset, source_type,
          (source_type='SYNTHETIC_SCALE_TEST') AS is_synthetic,
          event_message_count, unique_event_count, unique_detection_count
          FROM serving.dataset_activity_daily_summary WHERE {' AND '.join(conditions)}
          ORDER BY activity_date, source_dataset, source_type LIMIT %s"""
        params.append(query.limit)
        with self.connection() as connection:
            return list(connection.execute(statement, tuple(params)).fetchall())

    def lineage(self, lineage_root_id: str, page: PageQuery) -> dict[str, Any] | None:
        cursor = decode_cursor(page.cursor)
        conditions = ["lineage_root_id = %s"]
        params: list[Any] = [lineage_root_id]
        if cursor:
            conditions.append(f"({ACTIVITY_SQL}, event_id) > (%s, %s)")
            params.extend(cursor)
        statement = f"""SELECT {EVENT_COLUMNS} FROM serving.event_detail
          WHERE {' AND '.join(conditions)} ORDER BY {ACTIVITY_SQL}, event_id LIMIT %s"""
        params.append(page.limit + 1)
        summary_sql = f"""SELECT lineage_root_id, count(*) AS event_message_count,
          count(DISTINCT event_id) AS unique_event_count,
          count(DISTINCT detection_id) AS unique_detection_count,
          count(*) FILTER (WHERE source_type='NASA_ORIGINAL') AS original_message_count,
          count(*) FILTER (WHERE source_type='NASA_REPLAY') AS replay_message_count,
          count(*) FILTER (WHERE source_type='SYNTHETIC_SCALE_TEST') AS synthetic_message_count,
          min(event_timestamp) AS first_observation_time,
          max({ACTIVITY_SQL}) AS last_activity_time
          FROM serving.event_detail WHERE lineage_root_id=%s GROUP BY lineage_root_id"""
        with self.connection() as connection:
            summary = connection.execute(summary_sql, (lineage_root_id,)).fetchone()
            if summary is None:
                return None
            rows = list(connection.execute(statement, tuple(params)).fetchall())
        next_cursor = None
        if len(rows) > page.limit:
            boundary = rows[page.limit - 1]
            next_cursor = encode_cursor(boundary["activity_timestamp"], boundary["event_id"])
            rows = rows[: page.limit]
        return {"summary": summary, "events": rows, "next_cursor": next_cursor}

    def bbox(self, query: BoundingBoxQuery) -> dict[str, Any]:
        cursor = decode_cursor(query.cursor)
        conditions, filter_params = self._filters(query)
        conditions[0:0] = [
            "geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)",
            "ST_Intersects(geometry, ST_MakeEnvelope(%s, %s, %s, %s, 4326))",
            f"{ACTIVITY_SQL} >= %s AND {ACTIVITY_SQL} < %s",
        ]
        envelope = [query.min_longitude, query.min_latitude, query.max_longitude, query.max_latitude]
        params: list[Any] = envelope + envelope + [query.start_time, query.end_time] + filter_params
        if cursor:
            conditions.append(f"({ACTIVITY_SQL}, event_id) > (%s, %s)")
            params.extend(cursor)
        params.append(query.limit + 1)
        statement = f"""SELECT {EVENT_COLUMNS} FROM serving.event_detail
          WHERE {' AND '.join(conditions)} ORDER BY {ACTIVITY_SQL}, event_id LIMIT %s"""
        with self.connection() as connection:
            rows = list(connection.execute(statement, tuple(params)).fetchall())
        next_cursor = None
        if len(rows) > query.limit:
            boundary = rows[query.limit - 1]
            next_cursor = encode_cursor(boundary["activity_timestamp"], boundary["event_id"])
            rows = rows[: query.limit]
        features = [
            {
                "type": "Feature",
                "id": row["event_id"],
                "geometry": {"type": "Point", "coordinates": (row["longitude"], row["latitude"])},
                "properties": row,
            }
            for row in rows
        ]
        return {"type": "FeatureCollection", "features": features, "next_cursor": next_cursor}

    def bbox_sql_for_plan(self, query: BoundingBoxQuery) -> tuple[str, tuple[Any, ...]]:
        conditions, filter_params = self._filters(query)
        conditions[0:0] = [
            "geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)",
            "ST_Intersects(geometry, ST_MakeEnvelope(%s, %s, %s, %s, 4326))",
            f"{ACTIVITY_SQL} >= %s AND {ACTIVITY_SQL} < %s",
        ]
        envelope = [query.min_longitude, query.min_latitude, query.max_longitude, query.max_latitude]
        statement = f"SELECT event_id FROM serving.event_detail WHERE {' AND '.join(conditions)} LIMIT %s"
        return statement, tuple(envelope + envelope + [query.start_time, query.end_time] + filter_params + [query.limit])
