import os
import unittest
from datetime import timedelta

import psycopg
from fastapi.testclient import TestClient

from eo_event_platform.api.app import create_app
from eo_event_platform.api.models import BoundingBoxQuery
from eo_event_platform.api.repository import ApiRepository


DSN = os.environ.get("EO_API_DATABASE_DSN", "")


def plan_nodes(node):
    yield node
    for child in node.get("Plans", []):
        yield from plan_nodes(child)


@unittest.skipUnless(DSN, "EO_API_DATABASE_DSN is required for PostgreSQL integration")
class ApiPostgresIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repository = ApiRepository(DSN)
        cls.client = TestClient(create_app(cls.repository))
        with psycopg.connect(DSN, autocommit=True) as connection:
            cls.sample = connection.execute(
                """SELECT lineage_root_id, longitude, latitude,
                  coalesce(scheduled_replay_timestamp,event_timestamp) AS activity_timestamp
                  FROM serving.event_detail ORDER BY event_id LIMIT 1"""
            ).fetchone()

    def test_readiness_uses_non_owner_read_only_role(self):
        response = self.client.get("/health/ready")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["database_role"], "eo_api_runtime")
        self.assertTrue(response.json()["read_only"])
        with psycopg.connect(DSN, autocommit=True) as connection:
            role = connection.execute(
                """SELECT current_user, r.rolsuper,
                  pg_has_role(current_user,'eo_api_readonly','member')
                  FROM pg_roles r WHERE r.rolname=current_user"""
            ).fetchone()
        self.assertEqual(role, ("eo_api_runtime", False, True))

    def test_role_cannot_insert_update_delete_or_create(self):
        statements = (
            "INSERT INTO serving.event_detail(event_id) VALUES ('forbidden')",
            "UPDATE serving.event_detail SET source_dataset='forbidden' WHERE false",
            "DELETE FROM serving.event_detail WHERE false",
            "CREATE TABLE public.forbidden_phase8(id integer)",
        )
        for statement in statements:
            with self.subTest(statement=statement), psycopg.connect(DSN, autocommit=True) as connection:
                with self.assertRaises(psycopg.errors.ReadOnlySqlTransaction):
                    connection.execute(statement)

    def test_all_endpoints_preserve_replay_truth_and_activity_time(self):
        summary = self.client.get("/v1/summary")
        self.assertEqual(summary.status_code, 200, summary.text)
        self.assertEqual(summary.json()["event_message_count"], 1_000_000)
        self.assertEqual(summary.json()["unique_detection_count"], 10_000)
        self.assertEqual(summary.json()["replay_message_count"], 1_000_000)
        self.assertEqual(summary.json()["original_message_count"], 0)
        self.assertEqual(summary.json()["synthetic_message_count"], 0)

        activity = self.sample[3]
        filtered = self.client.get(
            "/v1/summary",
            params={
                "start_time": (activity - timedelta(minutes=1)).isoformat(),
                "end_time": (activity + timedelta(minutes=1)).isoformat(),
                "source_type": "NASA_REPLAY",
                "source_dataset": "VIIRS_SNPP_SP",
            },
        )
        self.assertEqual(filtered.status_code, 200, filtered.text)
        self.assertGreater(filtered.json()["event_message_count"], 0)
        self.assertEqual(filtered.json()["original_message_count"], 0)
        self.assertEqual(filtered.json()["synthetic_message_count"], 0)

        daily = self.client.get(
            "/v1/daily",
            params={
                "start_date": activity.date().isoformat(),
                "end_date": activity.date().isoformat(),
                "source_type": "NASA_REPLAY",
                "source_dataset": "VIIRS_SNPP_SP",
            },
        )
        self.assertEqual(daily.status_code, 200)
        self.assertTrue(daily.json()["items"])
        self.assertTrue(all(item["source_type"] == "NASA_REPLAY" for item in daily.json()["items"]))

        lineage = self.client.get(f"/v1/lineages/{self.sample[0]}", params={"limit": 10})
        self.assertEqual(lineage.status_code, 200)
        self.assertEqual(lineage.json()["summary"]["event_message_count"], 100)
        self.assertTrue(all(event["source_type"] == "NASA_REPLAY" for event in lineage.json()["events"]))
        self.assertIsNotNone(lineage.json()["next_cursor"])
        second_page = self.client.get(
            f"/v1/lineages/{self.sample[0]}",
            params={"limit": 10, "cursor": lineage.json()["next_cursor"]},
        )
        self.assertEqual(second_page.status_code, 200, second_page.text)
        first_ids = {event["event_id"] for event in lineage.json()["events"]}
        second_ids = {event["event_id"] for event in second_page.json()["events"]}
        self.assertTrue(first_ids.isdisjoint(second_ids))

    def test_bbox_is_bounded_geojson_and_uses_gist_plan(self):
        activity = self.sample[3]
        query = BoundingBoxQuery(
            min_longitude=max(-180, self.sample[1] - 0.01),
            min_latitude=max(-90, self.sample[2] - 0.01),
            max_longitude=min(180, self.sample[1] + 0.01),
            max_latitude=min(90, self.sample[2] + 0.01),
            start_time=activity - timedelta(minutes=1),
            end_time=activity + timedelta(minutes=1),
            source_type="NASA_REPLAY",
            source_dataset="VIIRS_SNPP_SP",
            limit=5,
        )
        response = self.client.get("/v1/events/bbox", params=query.model_dump(exclude_none=True))
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["type"], "FeatureCollection")
        self.assertLessEqual(len(body["features"]), 5)
        self.assertTrue(body["features"])
        self.assertEqual(body["features"][0]["geometry"]["type"], "Point")

        statement, params = self.repository.bbox_sql_for_plan(query)
        with psycopg.connect(DSN, autocommit=True) as connection:
            plan = connection.execute(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement,
                params,
            ).fetchone()[0][0]["Plan"]
        index_names = {node.get("Index Name") for node in plan_nodes(plan)}
        self.assertIn("event_detail_geometry_gist_idx", index_names)
