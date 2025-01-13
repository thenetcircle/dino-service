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

    async def test_delete_before_set_to_x_messages_ago(self):
        max_count = self.env.config.get(ConfigKeys.ROOM_MAX_HISTORY_COUNT, domain=ConfigKeys.HISTORY)
        max_days = self.env.config.get(ConfigKeys.ROOM_MAX_HISTORY_DAYS, domain=ConfigKeys.HISTORY)

        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        await self.send_message_to_group_from(group_id, BaseTest.USER_ID, amount=2 * max_count)
        histories = await self.histories_for(group_id, BaseTest.USER_ID, per_page=500)
        n_messages = len(histories["messages"])
        self.assertEqual(2 * max_count, n_messages)

        await self.user_joins_group(group_id, BaseTest.THIRD_USER_ID)

        # sent max_count once before joining, and then once after joining
        await self.send_message_to_group_from(group_id, BaseTest.THIRD_USER_ID, amount=max_count)
        histories = await self.histories_for(group_id, BaseTest.THIRD_USER_ID, per_page=500)
        n_messages = len(histories["messages"])
        self.assertEqual(2 * max_count, n_messages)

        # first user have all 3 * max_count messages, plus the action log of the join
        histories = await self.histories_for(group_id, BaseTest.USER_ID, per_page=500)
        n_messages = len(histories["messages"])
        self.assertEqual(3 * max_count + 1, n_messages)

        user_stats = await self.get_user_stats(group_id, BaseTest.THIRD_USER_ID)
        delete_before = to_dt(user_stats["stats"]["delete_before"])
        timedelta = utcnow_dt() - delete_before

        # too many messages, so it should _not_ set to max_days
        self.assertEqual(timedelta.days, 0)
