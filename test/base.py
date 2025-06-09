import asyncio
from unittest import IsolatedAsyncioTestCase
from uuid import uuid4 as uuid

import arrow

from dinofw.db.rdbms.schemas import UserGroupStatsBase
from test.mocks import FakeEnv


def async_test(coroutine):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(coroutine(*args, **kwargs))

    return wrapper


class BaseTest(IsolatedAsyncioTestCase):
    GROUP_ID = "8888-7777-6666"
    USER_ID = 1234
    OTHER_USER_ID = 8888
    THIRD_USER_ID = 4321
    FOURTH_USER_ID = 5555
    MESSAGE_PAYLOAD = '{"message": "test message"}'
    FILE_ID = str(uuid()).replace("-", "")
    FILE_STATUS = 1
    FILE_CONTEXT = '{"some-key":"some-value"}'
    LONG_AGO = arrow.Arrow.utcfromtimestamp(789_000_000).datetime

    def setUp(self) -> None:
        self.fake_env = FakeEnv()
        self.fake_env.db.stats[BaseTest.USER_ID] = [BaseTest._generate_user_group_stats()]

    @classmethod
    def _generate_user_group_stats(cls,) -> UserGroupStatsBase:
        # used when no `hide_before` is specified in a query
        return UserGroupStatsBase(
                group_id=cls.GROUP_ID,
                user_id=cls.USER_ID,
                last_read=cls.LONG_AGO,
                last_sent=cls.LONG_AGO,
                delete_before=cls.LONG_AGO,
                join_time=cls.LONG_AGO,
                last_updated_time=cls.LONG_AGO,
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
