"""Bounded, replaceable cache boundary for validated aggregate API queries."""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable, Protocol

from pydantic import BaseModel


DEFAULT_TTL_SECONDS = 60.0
DEFAULT_MAX_ENTRIES = 256
DEFAULT_MAX_ENTRY_BYTES = 64 * 1024
DEFAULT_MAX_TOTAL_BYTES = 4 * 1024 * 1024


class CacheBackend(Protocol):
    """Replaceable byte-oriented cache contract used by the API."""

    def get(self, key: str) -> bytes | None: ...

    def set(self, key: str, value: bytes, ttl_seconds: float) -> bool: ...


@dataclass(frozen=True)
class CacheSnapshot:
    entries: int
    total_bytes: int
    hits: int
    misses: int
    expirations: int
    evictions: int
    rejected_entries: int


@dataclass
class _Entry:
    value: bytes
    expires_at: float


class BoundedTTLCache:
    """Thread-safe LRU cache with TTL, entry-count, and byte-size bounds."""

    def __init__(
        self,
        *,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        max_entry_bytes: int = DEFAULT_MAX_ENTRY_BYTES,
        max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if min(max_entries, max_entry_bytes, max_total_bytes) <= 0:
            raise ValueError("cache bounds must be positive")
        if max_entry_bytes > max_total_bytes:
            raise ValueError("maximum entry size cannot exceed total cache size")
        self.max_entries = max_entries
        self.max_entry_bytes = max_entry_bytes
        self.max_total_bytes = max_total_bytes
        self._clock = clock
        self._entries: OrderedDict[str, _Entry] = OrderedDict()
        self._total_bytes = 0
        self._hits = 0
        self._misses = 0
        self._expirations = 0
        self._evictions = 0
        self._rejected_entries = 0
        self._lock = threading.RLock()

    def get(self, key: str) -> bytes | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._misses += 1
                return None
            if entry.expires_at <= self._clock():
                self._remove(key)
                self._expirations += 1
                self._misses += 1
                return None
            self._entries.move_to_end(key)
            self._hits += 1
            return entry.value

    def set(self, key: str, value: bytes, ttl_seconds: float) -> bool:
        if ttl_seconds <= 0:
            raise ValueError("cache TTL must be positive")
        if not isinstance(value, bytes):
            raise TypeError("cache values must be bytes")
        with self._lock:
            if len(value) > self.max_entry_bytes or len(value) > self.max_total_bytes:
                self._rejected_entries += 1
                return False
            existing = self._entries.get(key)
            if existing is not None:
                self._remove(key)
            while self._entries and (
                len(self._entries) >= self.max_entries
                or self._total_bytes + len(value) > self.max_total_bytes
            ):
                oldest_key = next(iter(self._entries))
                self._remove(oldest_key)
                self._evictions += 1
            self._entries[key] = _Entry(value=value, expires_at=self._clock() + ttl_seconds)
            self._total_bytes += len(value)
            return True

    def snapshot(self) -> CacheSnapshot:
        with self._lock:
            return CacheSnapshot(
                entries=len(self._entries), total_bytes=self._total_bytes,
                hits=self._hits, misses=self._misses, expirations=self._expirations,
                evictions=self._evictions, rejected_entries=self._rejected_entries,
            )

    def _remove(self, key: str) -> None:
        entry = self._entries.pop(key)
        self._total_bytes -= len(entry.value)


def deterministic_cache_key(namespace: str, query: BaseModel) -> str:
    """Hash only a validated request model into a stable versioned key."""

    canonical = json.dumps(
        query.model_dump(mode="json", exclude_none=False),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    digest = hashlib.sha256(canonical).hexdigest()
    return f"astrayan-api:v1:{namespace}:{digest}"


def cache_bypassed(cache_control: str | None) -> bool:
    if not cache_control:
        return False
    directives = {item.split("=", 1)[0].strip().lower() for item in cache_control.split(",")}
    return bool(directives & {"no-cache", "no-store"})
