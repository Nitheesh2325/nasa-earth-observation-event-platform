import unittest
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from eo_event_platform.api.app import create_app
from eo_event_platform.api.cache import BoundedTTLCache, cache_bypassed, deterministic_cache_key
from eo_event_platform.api.models import SummaryQuery


NOW = datetime(2026, 8, 1, tzinfo=timezone.utc)


class Clock:
    def __init__(self):
        self.value = 100.0

    def __call__(self):
        return self.value


class CountingRepository:
    def __init__(self):
        self.calls = {"readiness": 0, "summary": 0, "daily": 0, "lineage": 0, "bbox": 0}

    def readiness(self):
        self.calls["readiness"] += 1
        return {"status": "ready", "database": "reachable", "database_role": "eo_api_runtime", "read_only": True}

    def summary(self, _query):
        self.calls["summary"] += 1
        return {
            "event_message_count": 1, "unique_event_count": 1, "unique_detection_count": 1,
            "original_message_count": 0, "replay_message_count": 1, "synthetic_message_count": 0,
            "first_observation_time": NOW, "last_observation_time": NOW,
            "first_activity_time": NOW, "last_activity_time": NOW,
        }

    def daily(self, _query):
        self.calls["daily"] += 1
        return [{
            "activity_date": NOW.date(), "source_dataset": "VIIRS_SNPP_SP",
            "source_type": "NASA_REPLAY", "is_synthetic": False,
            "event_message_count": 1, "unique_event_count": 1, "unique_detection_count": 1,
        }]

    def lineage(self, _lineage, _page):
        self.calls["lineage"] += 1
        return None

    def bbox(self, _query):
        self.calls["bbox"] += 1
        return {"type": "FeatureCollection", "features": [], "next_cursor": None}


class FailingCache:
    def get(self, _key):
        raise RuntimeError("cache unavailable")

    def set(self, _key, _value, _ttl):
        raise RuntimeError("cache unavailable")


class CacheBoundaryTests(unittest.TestCase):
    def test_deterministic_key_uses_all_validated_parameters(self):
        first = SummaryQuery(source_type="NASA_REPLAY", source_dataset="VIIRS_SNPP_SP")
        equivalent = SummaryQuery(source_dataset="VIIRS_SNPP_SP", source_type="NASA_REPLAY")
        different = SummaryQuery(source_type="NASA_ORIGINAL", source_dataset="VIIRS_SNPP_SP")
        self.assertEqual(deterministic_cache_key("summary", first), deterministic_cache_key("summary", equivalent))
        self.assertNotEqual(deterministic_cache_key("summary", first), deterministic_cache_key("summary", different))
        self.assertNotEqual(deterministic_cache_key("summary", first), deterministic_cache_key("daily", first))

    def test_hit_miss_expiration_and_bounds(self):
        clock = Clock()
        cache = BoundedTTLCache(max_entries=2, max_entry_bytes=4, max_total_bytes=6, clock=clock)
        self.assertIsNone(cache.get("a"))
        self.assertTrue(cache.set("a", b"123", 10))
        self.assertEqual(cache.get("a"), b"123")
        clock.value += 10
        self.assertIsNone(cache.get("a"))
        self.assertFalse(cache.set("large", b"12345", 10))
        self.assertTrue(cache.set("b", b"123", 10))
        self.assertTrue(cache.set("c", b"456", 10))
        self.assertTrue(cache.set("d", b"789", 10))
        snapshot = cache.snapshot()
        self.assertLessEqual(snapshot.entries, 2)
        self.assertLessEqual(snapshot.total_bytes, 6)
        self.assertEqual(snapshot.hits, 1)
        self.assertEqual(snapshot.expirations, 1)
        self.assertEqual(snapshot.rejected_entries, 1)
        self.assertEqual(snapshot.evictions, 1)

    def test_aggregate_hit_miss_bypass_and_expiry(self):
        clock = Clock()
        repository = CountingRepository()
        cache = BoundedTTLCache(clock=clock)
        client = TestClient(create_app(repository, cache, cache_ttl_seconds=10))
        url = "/v1/daily?start_date=2026-08-01&end_date=2026-08-01"
        self.assertEqual(client.get(url).status_code, 200)
        self.assertEqual(client.get(url).status_code, 200)
        self.assertEqual(repository.calls["daily"], 1)
        self.assertEqual(client.get(url, headers={"Cache-Control": "no-cache"}).status_code, 200)
        self.assertEqual(repository.calls["daily"], 2)
        clock.value += 10
        self.assertEqual(client.get(url).status_code, 200)
        self.assertEqual(repository.calls["daily"], 3)

    def test_backend_failure_falls_back_to_repository(self):
        repository = CountingRepository()
        client = TestClient(create_app(repository, FailingCache()))
        first = client.get("/v1/summary")
        second = client.get("/v1/summary")
        self.assertEqual((first.status_code, second.status_code), (200, 200))
        self.assertEqual(first.json(), second.json())
        self.assertEqual(repository.calls["summary"], 2)

    def test_health_and_detail_endpoints_are_never_cached(self):
        repository = CountingRepository()
        client = TestClient(create_app(repository, BoundedTTLCache()))
        for _ in range(2):
            self.assertEqual(client.get("/health/ready").status_code, 200)
            self.assertEqual(client.get("/v1/lineages/missing").status_code, 404)
            response = client.get(
                "/v1/events/bbox?min_longitude=19&min_latitude=9&max_longitude=21&max_latitude=11"
                "&start_time=2026-08-01T00:00:00Z&end_time=2026-08-02T00:00:00Z"
            )
            self.assertEqual(response.status_code, 200)
        self.assertEqual(repository.calls["readiness"], 2)
        self.assertEqual(repository.calls["lineage"], 2)
        self.assertEqual(repository.calls["bbox"], 2)

    def test_invalid_request_is_not_cached_and_standard_bypass_is_explicit(self):
        repository = CountingRepository()
        cache = BoundedTTLCache()
        client = TestClient(create_app(repository, cache))
        self.assertEqual(client.get("/v1/daily?start_date=bad&end_date=2026-08-01").status_code, 422)
        self.assertEqual(repository.calls["daily"], 0)
        self.assertEqual(cache.snapshot().entries, 0)
        self.assertTrue(cache_bypassed("public, no-store"))
        self.assertTrue(cache_bypassed("NO-CACHE=max-age"))
        self.assertFalse(cache_bypassed("max-age=0"))


if __name__ == "__main__":
    unittest.main()
