from dinofw.rest.queries import GroupQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.utils.config import MessageTypes
from asyncio import sleep as async_sleep
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
        await async_sleep(0.1)

        unread_amount, n_unread_groups = await self.env.rest.user.count_unread(
            BaseDatabaseTest.USER_ID,
            GroupQuery(
                receiver_ids=[BaseDatabaseTest.OTHER_USER_ID],
                per_page=100
            ),
            session
        )
        self.assertEqual(0, unread_amount)
        self.assertEqual(0, n_unread_groups)

        unread_amount, n_unread_groups = await self.env.rest.user.count_unread(
            BaseDatabaseTest.OTHER_USER_ID,
            GroupQuery(
                receiver_ids=[BaseDatabaseTest.USER_ID],
                per_page=100
            ),
            session
        )
        self.assertEqual(1, unread_amount)
        self.assertEqual(1, n_unread_groups)

    @async_test
    async def test_unread_count_more_than_limit(self):
        session = self.env.session_maker()

        to_send = 5
        limit_to = 3

        for user_id in range(to_send):
            await self.env.rest.message.send_message_to_user(
                user_id + 1,
                SendMessageQuery(
                    receiver_id=BaseTest.OTHER_USER_ID,
                    message_type=MessageTypes.MESSAGE,
                    message_payload="some message"
                ),
                session
            )
        await async_sleep(0.1)

        unread_amount, n_unread_groups = await self.env.rest.user.count_unread(
            BaseDatabaseTest.OTHER_USER_ID,
            GroupQuery(per_page=limit_to),
            session
        )
        self.assertEqual(limit_to, unread_amount)
        self.assertEqual(limit_to, n_unread_groups)
