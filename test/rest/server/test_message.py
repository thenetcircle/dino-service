from unittest import TestCase

import asyncio

from dinofw.rest.server.message import MessageResource
from dinofw.rest.server.models import SendMessageQuery
from test.mocks import FakeEnv


def async_test(coroutine):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(coroutine(*args, **kwargs))
    return wrapper


class TestMessageResource(TestCase):
    GROUP_ID = '8888-7777-6666'
    USER_ID = 1234

    def setUp(self) -> None:
        self.resource = MessageResource(FakeEnv())

    @async_test
    async def test_save_new_message(self):
        query = SendMessageQuery(
            message_payload="a new message",
            message_type="text"
        )

        self.assertNotIn(TestMessageResource.GROUP_ID, self.resource.env.publisher.sent_messages)

        message = await self.resource.save_new_message(
            group_id=TestMessageResource.GROUP_ID,
            user_id=TestMessageResource.USER_ID,
            query=query,
            db=None  # noqa
        )
        self.assertEqual(1, len(self.resource.env.publisher.sent_messages[TestMessageResource.GROUP_ID]))

        self.assertIsNotNone(message.message_id)

        last_sent = self.resource.env.db.stats[TestMessageResource.USER_ID].last_sent
        last_read = self.resource.env.db.stats[TestMessageResource.USER_ID].last_read
        join_time = self.resource.env.db.stats[TestMessageResource.USER_ID].join_time
        self.assertIsNotNone(last_sent)
        self.assertIsNotNone(last_read)
        self.assertIsNotNone(join_time)

        await self.resource.save_new_message(
            group_id=TestMessageResource.GROUP_ID,
            user_id=TestMessageResource.USER_ID,
            query=query,
            db=None  # noqa
        )
        self.assertEqual(2, len(self.resource.env.publisher.sent_messages[TestMessageResource.GROUP_ID]))

        new_last_sent = self.resource.env.db.stats[TestMessageResource.USER_ID].last_sent
        new_last_read = self.resource.env.db.stats[TestMessageResource.USER_ID].last_read
        new_join_time = self.resource.env.db.stats[TestMessageResource.USER_ID].join_time
        self.assertNotEqual(last_sent, new_last_sent)
        self.assertNotEqual(last_read, new_last_read)
        self.assertEqual(join_time, new_join_time)
