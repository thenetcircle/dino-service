from dinofw.rest.queries import MessageQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.rest.queries import UpdateUserGroupStats
from dinofw.utils.config import MessageTypes

from test.base import BaseTest
from test.base import async_test
from test.functional.base_db import BaseDatabaseTest
from test.functional.base_functional import BaseServerRestApi


class TestUnreadCount(BaseServerRestApi):
    @async_test
    async def test_unread_count_0_and_1(self):
        session = self.env.session_maker()

        await self.env.rest.message.send_message_to_user(
            BaseTest.USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.OTHER_USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )

        unread_amount, n_unread_groups = self.env.rest.user.count_unread(
            BaseDatabaseTest.USER_ID, session
        )
        self.assertEqual(0, unread_amount)
        self.assertEqual(0, n_unread_groups)

        unread_amount, n_unread_groups = self.env.rest.user.count_unread(
            BaseDatabaseTest.OTHER_USER_ID, session
        )
        self.assertEqual(1, unread_amount)
        self.assertEqual(1, n_unread_groups)

    @async_test
    async def test_count_total_unread(self):
        session = self.env.session_maker()

        unread_count, n_unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(0, unread_count)
        self.assertEqual(0, n_unread_groups)

        await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )

        unread_count, n_unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(1, unread_count)
        self.assertEqual(1, n_unread_groups)

    @async_test
    async def test_count_total_unread_included_bookmark(self):
        session = self.env.session_maker()

        unread_count, n_unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(0, unread_count)
        self.assertEqual(0, n_unread_groups)

        await self.env.rest.message.send_message_to_user(
            BaseTest.OTHER_USER_ID,
            SendMessageQuery(
                receiver_id=BaseTest.USER_ID,
                message_type=MessageTypes.MESSAGE,
                message_payload="some message"
            ),
            session
        )

        # send 3 messages, so we know it's the bookmarking that gives +1 unread
        group_id = None
        for _ in range(3):
            message = await self.env.rest.message.send_message_to_user(
                BaseTest.OTHER_USER_ID + 1,
                SendMessageQuery(
                    receiver_id=BaseTest.USER_ID,
                    message_type=MessageTypes.MESSAGE,
                    message_payload="some message"
                ),
                session
            )
            group_id = message.group_id

        # two groups, one each
        unread_count, n_unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(4, unread_count)
        self.assertEqual(2, n_unread_groups)

        # mark as read, so should have 0 unread for this group, 1 unread in total
        await self.env.rest.group.histories(
            group_id, BaseTest.USER_ID, MessageQuery(per_page=30, since=0), session
        )

        unread_count, n_unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(1, unread_count)
        self.assertEqual(1, n_unread_groups)

        # bookmark the one we just read
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.USER_ID, UpdateUserGroupStats(bookmark=True), session
        )

        # should have 1 unread message, and 1 bookmarked without unread (counting as 1 unread)
        unread_count, n_unread_groups = self.env.db.count_total_unread(BaseTest.USER_ID, session)
        self.assertEqual(2, unread_count)
        self.assertEqual(2, n_unread_groups)

    def test_count_total_unread_cached(self):
        session = self.env.session_maker()

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertIsNone(cached_unread_count)
        self.assertIsNone(cached_unread_groups)

        unread_count, unread_groups = self.env.rest.user.count_unread(BaseTest.USER_ID, session)
        self.assertEqual(0, unread_count)
        self.assertEqual(0, unread_groups)

        cached_unread_count, cached_unread_groups = self.env.cache.get_total_unread_count(BaseTest.USER_ID)
        self.assertEqual(unread_count, cached_unread_count)
        self.assertEqual(unread_groups, cached_unread_groups)
