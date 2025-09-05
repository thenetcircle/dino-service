import asyncio
from typing import List, Optional

from dinofw.utils.config import GroupTypes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestPublicGroupsCache(BaseServerRestApi):
    async def get_public_groups_raw(
        self,
        include_archived: bool = False,
        admin_id: Optional[int] = None,
        spoken_languages: Optional[List[str]] = None,
        users: Optional[List[int]] = None,
    ):
        """
        Raw helper that returns (status_code, headers, json_payload).
        Keeps your existing get_public_groups() unchanged.
        """
        data = {"include_archived": include_archived, "admin_id": admin_id}
        if spoken_languages:
            data["spoken_languages"] = spoken_languages
        if users:
            data["users"] = users

        resp = await self.client.post("/v1/groups/public", json=data)
        resp.raise_for_status()
        return resp.status_code, resp.headers, resp.json()

    async def test_cache_hit_path(self):
        self.assertEqual(0, len(await self.env.cache.redis.keys()))

        # Arrange: create one public and one private group
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[BaseTest.OTHER_USER_ID, BaseTest.THIRD_USER_ID],
            group_type=GroupTypes.PRIVATE_GROUP,
        )
        public_gid = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[BaseTest.OTHER_USER_ID, BaseTest.THIRD_USER_ID],
            group_type=GroupTypes.PUBLIC_ROOM,
            language="en"
        )

        await self.assert_deleted_groups_for_user(0)
        await self.assert_groups_for_user(1)
        await self.assert_public_groups_for_user(1)  # with 'users' it by-passes the cache

        # Act 1: first call populates cache
        _, headers1, body1 = await self.get_public_groups_raw(spoken_languages=["de", "en"])
        self.assertEqual(1, len(body1))
        self.assertEqual(public_gid, body1[0]["group_id"])
        # first call is a miss; allow either "miss-refreshing" or "nocache"
        self.assertIn(headers1.get("X-Cache", "nocache"), ("miss-refreshing", "nocache"))

        # Act 2: second call should be a cache hit
        _, headers2, body2 = await self.get_public_groups_raw(spoken_languages=["en", "de"])
        self.assertEqual(body1, body2)  # same payload
        self.assertEqual("hit", headers2.get("X-Cache"))

    async def test_cache_skip_conditions(self):
        # create a public group
        await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[BaseTest.OTHER_USER_ID],
            group_type=GroupTypes.PUBLIC_ROOM,
            language="en"
        )

        # admin_id present -> skip cache
        _, headers, _ = await self.get_public_groups_raw(admin_id=123, spoken_languages=["de"])
        self.assertEqual(headers.get("X-Cache"), "nocache")

        # users present -> skip cache
        _, headers2, _ = await self.get_public_groups_raw(users=[BaseTest.OTHER_USER_ID], spoken_languages=["de"])
        self.assertEqual(headers2.get("X-Cache"), "nocache")

    async def test_cache_expiry_and_background_refresh(self):
        # Make SOFT_TTL small for this test
        from dinofw.rest import groups_cache as cachemod
        from unittest.mock import patch
        import time

        # Arrange: one public group initially
        gid1 = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[BaseTest.OTHER_USER_ID],
            group_type=GroupTypes.PUBLIC_ROOM,
            language="de"
        )


        # Patch BEFORE the first call so the populated entry has TTL=1s and jitter=0
        orig_ttl = cachemod.SOFT_TTL_SEC
        try:
            cachemod.SOFT_TTL_SEC = 1

            with patch.object(cachemod.random, "randint", return_value=0):
                # 1) First call populates cache (miss -> store with soft=now+1s)
                _, headers1, body1 = await self.get_public_groups_raw(spoken_languages=["de"])
                self.assertEqual(1, len(body1))
                self.assertIn(headers1.get("X-Cache", "nocache"), ("miss-refreshing", "nocache", "hit", "stale"))

                # 2) Wait for soft TTL to expire
                await asyncio.sleep(1.1)

                # 3) Change underlying data
                gid2 = await self.create_and_join_group(
                    user_id=BaseTest.USER_ID,
                    users=[BaseTest.THIRD_USER_ID],
                    group_type=GroupTypes.PUBLIC_ROOM,
                    language="de",
                )
                self.assertNotEqual(gid1, gid2)

                # 4) Second call should serve stale and trigger background refresh
                _, headers2, body2 = await self.get_public_groups_raw(spoken_languages=["de"])
                self.assertIn(headers2.get("X-Cache"), ("stale", "stale-refreshing", "hit"))
                self.assertEqual(1, len(body2))  # still stale content

                # 5) Poll until refresh lands (be generous to avoid flakiness)
                deadline = time.monotonic() + 2.0
                last_headers, last_body = headers2, body2
                while time.monotonic() < deadline:
                    _, h, b = await self.get_public_groups_raw(spoken_languages=["de"])
                    last_headers, last_body = h, b
                    if h.get("X-Cache") == "hit" and len(b) == 2:
                        break
                    await asyncio.sleep(0.05)
                else:
                    self.fail(
                        f"cache not refreshed in time; last X-Cache={last_headers.get('X-Cache')} len={len(last_body)}")

                self.assertCountEqual([g["group_id"] for g in last_body], [gid1, gid2])
        finally:
            cachemod.SOFT_TTL_SEC = orig_ttl
