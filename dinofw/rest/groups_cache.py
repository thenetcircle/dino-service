import json
import random
import time
from abc import ABC
from typing import List, Optional, Tuple

import orjson
from loguru import logger

from dinofw.rest.models import Group

# ---- Cache constants (can be monkeypatched in tests) ----
SOFT_TTL_SEC = 60      # serve-as-fresh duration (+ jitter)
HARD_TTL_SEC = 600     # safety TTL in Redis, even if refresh fails
LOCK_TTL_SEC = 15      # single-flight lock TTL
CACHE_PREFIX = "public_groups:v2"  # bump to invalidate old schema


def dumps_to_bytes(obj: object) -> bytes:
    """Serialize to UTF-8 JSON bytes. Uses orjson if available."""
    if orjson is not None:
        return orjson.dumps(obj)
    # stdlib json: compact and utf-8
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _langs_key(spoken_languages: Optional[List[str]]) -> str:
    """Normalize languages to stable part of the cache key."""
    if not spoken_languages:
        return "none"
    langs = []
    for s in spoken_languages:
        if isinstance(s, str) and len(s) == 2 and s.isascii():
            langs.append(s.lower())
    if not langs:
        return "none"
    # stable, deduped, order-independent
    return ",".join(sorted(set(langs)))


def _to_int_from_meta(v) -> int:
    """Convert Redis meta value (bytes|str|int) to int safely."""
    if isinstance(v, (bytes, bytearray)):
        return int(v.decode("utf-8"))
    return int(v)  # works for str or int


def _ensure_bytes(v) -> bytes:
    """Ensure value is bytes for HTTP Response body."""
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, str):
        return v.encode("utf-8")
    # Not expected: fall back to JSON
    return json.dumps(v, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


class PublicGroupsCacheMixin(ABC):
    """
    Mixin with small, focused helpers for caching the /groups/public response.
    Assumes `self.env.cache.redis` is an async Redis client and you have:
      - _compute_public_groups(query, db) -> List[Group]
    """

    def __init__(self):
        self.env = None

    async def compute_public_groups(self, query, db) -> List[Group]:
        """Override in the main class."""
        raise NotImplementedError()

    # ---------- cache key helpers ----------

    def _base_cache_key(self, query) -> str:
        langs = getattr(query, "spoken_languages", None)
        return f"{CACHE_PREFIX}:{_langs_key(langs)}"

    def _keys(self, base: str) -> Tuple[str, str, str]:
        # meta stores only an int (soft_expire), data stores raw JSON bytes
        return f"{base}:meta", f"{base}:data", f"{base}:lock"

    # ---------- public entry points ----------

    async def get_cached_public_groups_raw(
        self, query
    ) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
        """
        Try to fetch cached raw JSON. Returns (raw_bytes, status, base_key)
        status ∈ { "hit", "stale", "stale-refreshing", "miss-refreshing", "miss-wait", None }
        """
        # Skip cache if admin/users filters are present
        if getattr(query, "admin_id", None) is not None or getattr(query, "users", None):
            return None, None, None

        redis = self.env.cache.redis
        base = self._base_cache_key(query)
        meta_key, data_key, lock_key = self._keys(base)

        # fetch small meta + large data in one go
        meta, data = await redis.mget(meta_key, data_key)
        now = int(time.time())

        if meta and data:
            # decode tiny meta
            try:
                soft_expire = _to_int_from_meta(meta)
            except Exception:
                soft_expire = 0

            data_bytes = _ensure_bytes(data)

            if soft_expire >= now:
                return data_bytes , "hit", base

            # stale: one worker refreshes; others serve stale
            acquired = await redis.set(lock_key, "1", nx=True, ex=LOCK_TTL_SEC)
            if acquired:
                return data_bytes , "stale-refreshing", base
            return data_bytes , "stale", base

        # miss
        acquired = await redis.set(lock_key, "1", nx=True, ex=LOCK_TTL_SEC)
        if acquired:
            return None, "miss-refreshing", base
        return None, "miss-wait", base

    async def set_cached_public_groups_raw(self, base: str, raw_json: bytes) -> None:
        """Write data+meta atomically and drop the lock."""
        redis = self.env.cache.redis
        meta_key, data_key, lock_key = self._keys(base)
        soft = str(int(time.time()) + SOFT_TTL_SEC + random.randint(0, 30)).encode("utf-8")

        p = redis.pipeline()
        p.set(data_key, raw_json, ex=HARD_TTL_SEC)
        p.set(meta_key, soft, ex=HARD_TTL_SEC)
        p.delete(lock_key)
        await p.execute()

    async def refresh_cache_in_background(self, base: str, query) -> None:
        """
        Compute → serialize → set cache, then release the lock.
        Safe to fire-and-forget with asyncio.create_task.
        """
        try:
            with self.env.SessionLocal() as db:
                groups = await self.compute_public_groups(query, db)
                # dump dicts, not models, for stable JSON shape
                items = []
                for g in groups:
                    if hasattr(g, "model_dump"):
                        items.append(g.model_dump(exclude_none=True))  # pydantic v2
                    else:
                        items.append(g.dict(exclude_none=True))        # pydantic v1
                raw = dumps_to_bytes(items)
                await self.set_cached_public_groups_raw(base, raw)
        except Exception as e:
            # If background refresh fails, lock TTL will expire naturally.
            logger.exception(e)
