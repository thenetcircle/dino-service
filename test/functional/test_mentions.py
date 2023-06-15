import arrow

from dinofw.rest.queries import MessageQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.rest.queries import UpdateUserGroupStats
from dinofw.utils.config import MessageTypes

from test.base import BaseTest
from test.base import async_test
from test.functional.base_functional import BaseServerRestApi


class TestMentions(BaseServerRestApi):
    @async_test
    async def test_mentions_increases(self):
        session = self.env.session_maker()

        send_query = SendMessageQuery(
            receiver_id=BaseTest.OTHER_USER_ID,
            message_type=MessageTypes.MESSAGE,
            message_payload="some message"
        )
        message = await self.env.rest.message.send_message_to_user(BaseTest.USER_ID, send_query, session)
        group_id = message.group_id

        # first message we didn't mention this user, so should have 0 mentions
        stats = await self.env.rest.group.get_user_group_stats(group_id, BaseTest.OTHER_USER_ID, session)
        self.assertEqual(0, stats.mentions)

        send_query.mention_user_ids = [BaseTest.OTHER_USER_ID]
        await self.env.rest.message.send_message_to_user(BaseTest.USER_ID, send_query, session)

        # should be 1 now that we mentioned the user
        stats = await self.env.rest.group.get_user_group_stats(group_id, BaseTest.OTHER_USER_ID, session)
        self.assertEqual(1, stats.mentions)

    @async_test
    async def test_mentions_reset_on_get_history(self):
        session = self.env.session_maker()

        send_query = SendMessageQuery(
            receiver_id=BaseTest.OTHER_USER_ID,
            message_type=MessageTypes.MESSAGE,
            message_payload="some message",
            mention_user_ids=[BaseTest.OTHER_USER_ID]
        )
        message = await self.env.rest.message.send_message_to_user(BaseTest.USER_ID, send_query, session)
        group_id = message.group_id

        stats = await self.env.rest.group.get_user_group_stats(group_id, BaseTest.OTHER_USER_ID, session)
        self.assertEqual(1, stats.mentions)

        message_query = MessageQuery(since=0, per_page=100)
        await self.env.rest.group.histories(group_id, BaseTest.OTHER_USER_ID, message_query, session)

        # should have reset to 0 after we get history
        stats = await self.env.rest.group.get_user_group_stats(group_id, BaseTest.OTHER_USER_ID, session)
        self.assertEqual(0, stats.mentions)

    @async_test
    async def test_mentions_reset_on_update_last_read(self):
        session = self.env.session_maker()

        send_query = SendMessageQuery(
            receiver_id=BaseTest.OTHER_USER_ID,
            message_type=MessageTypes.MESSAGE,
            message_payload="some message",
            mention_user_ids=[BaseTest.OTHER_USER_ID]
        )
        message = await self.env.rest.message.send_message_to_user(BaseTest.USER_ID, send_query, session)
        group_id = message.group_id

        stats = await self.env.rest.group.get_user_group_stats(group_id, BaseTest.OTHER_USER_ID, session)
        self.assertEqual(1, stats.mentions)

        message_query = UpdateUserGroupStats(last_read_time=arrow.utcnow().float_timestamp)
        await self.env.rest.group.update_user_group_stats(group_id, BaseTest.OTHER_USER_ID, message_query, session)

        # should have reset to 0 after we mark the group as read (updating last_read_at)
        stats = await self.env.rest.group.get_user_group_stats(group_id, BaseTest.OTHER_USER_ID, session)
        self.assertEqual(0, stats.mentions)
