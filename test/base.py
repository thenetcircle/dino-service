import asyncio
from unittest import TestCase


def async_test(coroutine):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(coroutine(*args, **kwargs))
    return wrapper


class BaseTest(TestCase):
    GROUP_ID = '8888-7777-6666'
    USER_ID = 1234
