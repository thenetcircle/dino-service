from unittest import TestCase

from dinofw.rest.server.message import MessageResource
from dinofw.rest.server.models import SendMessageQuery, MessageQuery, EditMessageQuery
from test.base import async_test, BaseTest
from test.mocks import FakeEnv


class TestMessageResource(BaseTest):
    def setUp(self) -> None:
        self.resource = MessageResource(FakeEnv())

    @async_test
    async def test_save_new_message(self):
        query = SendMessageQuery(
            message_payload="a new message",
            message_type="text"
        )

        self.assertNotIn(BaseTest.GROUP_ID, self.resource.env.publisher.sent_messages)

        message = await self.resource.save_new_message(
            group_id=BaseTest.GROUP_ID,
            user_id=BaseTest.USER_ID,
            query=query,
            db=None  # noqa
        )
        self.assertEqual(1, len(self.resource.env.publisher.sent_messages[BaseTest.GROUP_ID]))

        self.assertIsNotNone(message.message_id)

        last_sent = self.resource.env.db.stats[BaseTest.USER_ID].last_sent
        last_read = self.resource.env.db.stats[BaseTest.USER_ID].last_read
        join_time = self.resource.env.db.stats[BaseTest.USER_ID].join_time
        self.assertIsNotNone(last_sent)
        self.assertIsNotNone(last_read)
        self.assertIsNotNone(join_time)

        await self.resource.save_new_message(
            group_id=BaseTest.GROUP_ID,
            user_id=BaseTest.USER_ID,
            query=query,
            db=None  # noqa
        )
        self.assertEqual(2, len(self.resource.env.publisher.sent_messages[BaseTest.GROUP_ID]))

        new_last_sent = self.resource.env.db.stats[BaseTest.USER_ID].last_sent
        new_last_read = self.resource.env.db.stats[BaseTest.USER_ID].last_read
        new_join_time = self.resource.env.db.stats[BaseTest.USER_ID].join_time
        self.assertNotEqual(last_sent, new_last_sent)
        self.assertNotEqual(last_read, new_last_read)
        self.assertEqual(join_time, new_join_time)

    @async_test
    async def test_messages_in_group(self):
        message_query = MessageQuery(
            per_page=10,
        )
        send_query = SendMessageQuery(
            message_payload="a new message",
            message_type="text"
        )

        messages = await self.resource.messages_in_group(BaseTest.GROUP_ID, message_query)
        self.assertEqual(0, len(messages))

        await self.resource.save_new_message(
            group_id=BaseTest.GROUP_ID,
            user_id=BaseTest.USER_ID,
            query=send_query,
            db=None  # noqa
        )

        messages = await self.resource.messages_in_group(BaseTest.GROUP_ID, message_query)
        self.assertEqual(1, len(messages))

    @async_test
    async def test_messages_for_user(self):
        message_query = MessageQuery(
            per_page=10,
        )
        send_query = SendMessageQuery(
            message_payload="a new message",
            message_type="text"
        )

        messages = await self.resource.messages_for_user(
            BaseTest.GROUP_ID,
            BaseTest.USER_ID,
            message_query
        )
        self.assertEqual(0, len(messages))

        await self.resource.save_new_message(
            group_id=BaseTest.GROUP_ID,
            user_id=BaseTest.USER_ID,
            query=send_query,
            db=None  # noqa
        )

        messages = await self.resource.messages_for_user(
            BaseTest.GROUP_ID,
            BaseTest.USER_ID,
            message_query
        )
        self.assertEqual(1, len(messages))

    @async_test
    async def test_edit_message(self):
        new_text = "edited message"
        old_text = "a new message"

        send_query = SendMessageQuery(
            message_payload=old_text,
            message_type="text"
        )
        edit_query = EditMessageQuery(
            message_payload=new_text,
        )
        message_query = MessageQuery(
            per_page=10,
        )

        message = await self.resource.save_new_message(
            group_id=BaseTest.GROUP_ID,
            user_id=BaseTest.USER_ID,
            query=send_query,
            db=None  # noqa
        )
        messages = await self.resource.messages_for_user(
            BaseTest.GROUP_ID,
            BaseTest.USER_ID,
            message_query
        )
        self.assertEqual(messages[0].message_payload, old_text)
        self.assertIsNone(messages[0].updated_at)

        await self.resource.edit_message(
            BaseTest.GROUP_ID,
            BaseTest.USER_ID,
            message.message_id,
            edit_query
        )

        messages = await self.resource.messages_for_user(
            BaseTest.GROUP_ID,
            BaseTest.USER_ID,
            message_query
        )
        self.assertEqual(messages[0].message_payload, new_text)
        self.assertIsNotNone(messages[0].updated_at)
