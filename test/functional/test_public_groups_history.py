from dinofw.utils import to_dt
from dinofw.utils import utcnow_dt
from dinofw.utils.config import GroupTypes, ConfigKeys
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi
import arrow


class TestPublicGroups(BaseServerRestApi):
    async def test_created_at_set_to_x_days_ago(self):
        # 5 days and 10 msgs in mocks
        max_days = self.env.config.get(ConfigKeys.ROOM_MAX_HISTORY_DAYS, domain=ConfigKeys.HISTORY)
        half_max_count = int(self.env.config.get(ConfigKeys.ROOM_MAX_HISTORY_COUNT, domain=ConfigKeys.HISTORY) / 2)

        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        user_stats = await self.get_user_stats(group_id, BaseTest.USER_ID)
        delete_before = to_dt(user_stats["stats"]["delete_before"])
        timedelta = utcnow_dt() - delete_before
        self.assertEqual(max_days, timedelta.days)

        await self.send_message_to_group_from(group_id, BaseTest.USER_ID, amount=half_max_count)
        await self.user_joins_group(group_id, BaseTest.THIRD_USER_ID)

        # new user has joined, but not enough msgs, so delete_before should be max_days ago still
        await self.user_joins_group(group_id, BaseTest.THIRD_USER_ID)
        user_stats = await self.get_user_stats(group_id, BaseTest.THIRD_USER_ID)
        delete_before = to_dt(user_stats["stats"]["delete_before"])
        timedelta = utcnow_dt() - delete_before
        self.assertEqual(max_days, timedelta.days)
