import arrow

from dinofw.utils import to_ts
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestHighlightTime(BaseServerRestApi):
    async def test_receiver_highlight_time(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        stats = (await self.groups_for_user(BaseTest.USER_ID))[0]["stats"]
        self.assertEqual(self.long_ago, stats["receiver_highlight_time"])

        now_plus_2_days = arrow.utcnow().shift(days=2).datetime
        now_plus_2_days = to_ts(now_plus_2_days)
        await self.highlight_group_for_user(
            group_message["group_id"],
            user_id=BaseTest.OTHER_USER_ID,
            highlight_time=now_plus_2_days
        )

        stats = (await self.groups_for_user(BaseTest.USER_ID))[0]["stats"]
        self.assertEqual(now_plus_2_days, stats["receiver_highlight_time"])

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID))[0]["stats"]
        self.assertEqual(now_plus_2_days, stats["highlight_time"])

    async def test_update_last_read_removes_highlight_time(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        stats = (await self.groups_for_user(BaseTest.USER_ID))[0]["stats"]
        self.assertEqual(self.long_ago, stats["receiver_highlight_time"])

        now_plus_2_days = arrow.utcnow().shift(days=2).datetime
        now_plus_2_days = to_ts(now_plus_2_days)
        await self.highlight_group_for_user(
            group_message["group_id"],
            user_id=BaseTest.OTHER_USER_ID,
            highlight_time=now_plus_2_days
        )

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID))[0]["stats"]
        self.assertEqual(now_plus_2_days, stats["highlight_time"])

        await self.update_last_read(group_message["group_id"], BaseTest.OTHER_USER_ID)

        stats = (await self.groups_for_user(BaseTest.OTHER_USER_ID))[0]["stats"]
        self.assertEqual(self.long_ago, stats["highlight_time"])
