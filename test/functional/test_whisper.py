import asyncio

from dinofw.rest.queries import CreateGroupQuery, UserStatsQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.utils.config import MessageTypes, GroupTypes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestWhispers(BaseServerRestApi):
    @BaseServerRestApi.init_db_session
    async def test_whisper_filters_out_receivers_to_increase_unread_for(self):
        session = self.env.db_session

        group_query = CreateGroupQuery(
            users=[BaseTest.OTHER_USER_ID, BaseTest.THIRD_USER_ID],
            group_name="test group",
            group_type=GroupTypes.PUBLIC_ROOM
        )
        group = await self.env.rest.group.create_new_group(BaseTest.USER_ID, group_query, session)
        group_id = group.group_id

        send_query = SendMessageQuery(
            receiver_id=BaseTest.OTHER_USER_ID,
            message_type=MessageTypes.MESSAGE,
            message_payload="some message"
        )
        await self.env.rest.message.send_message_to_group(group_id, BaseTest.USER_ID, send_query, session)

        stats_query = UserStatsQuery(count_unread=True)

        stats = await self.env.rest.user.get_user_stats(BaseTest.OTHER_USER_ID, stats_query, session)
        self.assertEqual(stats.unread_amount, 1)

        stats = await self.env.rest.user.get_user_stats(BaseTest.THIRD_USER_ID, stats_query, session)
        self.assertEqual(stats.unread_amount, 1)

        # now send the whisper message to OTHER_USER_ID, only that user should have unread increased
        send_query = SendMessageQuery(
            receiver_id=BaseTest.OTHER_USER_ID,
            message_type=MessageTypes.MESSAGE,
            message_payload="some message",
            context=f'{{"action":64,"whisper":[{{"id":{BaseTest.OTHER_USER_ID},"nickname":"vipleo"}}]}}'
        )
        await self.env.rest.message.send_message_to_group(group_id, BaseTest.USER_ID, send_query, session)

        # this one should have increased
        stats = await self.env.rest.user.get_user_stats(BaseTest.OTHER_USER_ID, stats_query, session)
        self.assertEqual(stats.unread_amount, 2)

        # ...while this one should NOT have increased
        stats = await self.env.rest.user.get_user_stats(BaseTest.THIRD_USER_ID, stats_query, session)
        self.assertEqual(stats.unread_amount, 1)

    @BaseServerRestApi.init_db_session
    async def test_whisper_does_not_update_last_updated_at_for_non_mentioned_users(self):
        session = self.env.db_session

        group_query = CreateGroupQuery(
            users=[BaseTest.OTHER_USER_ID, BaseTest.THIRD_USER_ID],
            group_name="test group",
            group_type=GroupTypes.PUBLIC_ROOM
        )
        group = await self.env.rest.group.create_new_group(BaseTest.USER_ID, group_query, session)
        group_id = group.group_id

        send_query = SendMessageQuery(
            receiver_id=BaseTest.OTHER_USER_ID,
            message_type=MessageTypes.MESSAGE,
            message_payload="some message"
        )
        await self.env.rest.message.send_message_to_group(group_id, BaseTest.USER_ID, send_query, session)

        stats = await self.env.rest.group.get_user_group_stats(group_id, BaseTest.OTHER_USER_ID, session)
        last_updated_time_for_other_user = stats.last_updated_time

        stats = await self.env.rest.group.get_user_group_stats(group_id, BaseTest.THIRD_USER_ID, session)
        last_updated_time_for_third_user = stats.last_updated_time

        # just to ensure we don't get the exact same time back
        await asyncio.sleep(0.02)

        # now send the whisper message to OTHER_USER_ID, only that user should update time updated
        send_query = SendMessageQuery(
            receiver_id=BaseTest.OTHER_USER_ID,
            message_type=MessageTypes.MESSAGE,
            message_payload="some message",
            context=f'{{"action":64,"whisper":[{{"id":{BaseTest.OTHER_USER_ID},"nickname":"vipleo"}}]}}'
        )
        await self.env.rest.message.send_message_to_group(group_id, BaseTest.USER_ID, send_query, session)

        # this one should have been updated
        stats = await self.env.rest.group.get_user_group_stats(group_id, BaseTest.OTHER_USER_ID, session)
        self.assertGreater(stats.last_updated_time, last_updated_time_for_other_user)

        # ...while this one should NOT have been updated
        stats = await self.env.rest.group.get_user_group_stats(group_id, BaseTest.THIRD_USER_ID, session)
        self.assertEqual(stats.last_updated_time, last_updated_time_for_third_user)
