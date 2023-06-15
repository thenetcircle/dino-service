import asyncio
from unittest import TestCase
from uuid import uuid4 as uuid

import arrow

from dinofw.db.rdbms.schemas import UserGroupStatsBase
from test.mocks import FakeEnv


def async_test(coroutine):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(coroutine(*args, **kwargs))

    return wrapper


class BaseTest(TestCase):
    GROUP_ID = "8888-7777-6666"
    USER_ID = 1234
    OTHER_USER_ID = 8888
    THIRD_USER_ID = 4321
    MESSAGE_PAYLOAD = "test message"
    FILE_ID = str(uuid()).replace("-", "")
    FILE_STATUS = 1
    FILE_CONTEXT = '{"some-key":"some-value"}'

    def setUp(self) -> None:
        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        long_ago = arrow.Arrow.utcfromtimestamp(beginning_of_1995).datetime

        self.fake_env = FakeEnv()
        self.fake_env.db.stats[BaseTest.USER_ID] = [
            UserGroupStatsBase(
                group_id=BaseTest.GROUP_ID,
                user_id=BaseTest.USER_ID,
                last_read=long_ago,
                last_sent=long_ago,
                delete_before=long_ago,
                join_time=long_ago,
                last_updated_time=long_ago,
                hide=False,
                pin=False,
                bookmark=False,
                deleted=False,
                unread_count=0,
                mentions=0,
                notifications=True,
                sent_message_count=-1,
                kicked=False,
            )
        ]
