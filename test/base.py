import asyncio
from datetime import datetime as dt
import pytz
from unittest import TestCase

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

    def setUp(self) -> None:
        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        long_ago = dt.utcfromtimestamp(beginning_of_1995)
        long_ago = long_ago.replace(tzinfo=pytz.UTC)

        self.fake_env = FakeEnv()
        self.fake_env.db.stats[BaseTest.USER_ID] = [UserGroupStatsBase(
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
        )]
