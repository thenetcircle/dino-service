from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestLastReadTime(BaseServerRestApi):
    @BaseServerRestApi.init_db_session
    async def test_get_oldest_last_read_in_group(self):
        session = self.env.db_session

        message = await self.send_1v1_message()
        group_id = message["group_id"]

        last_read_cache = await self.env.cache.get_last_read_in_group_oldest(group_id)
        self.assertIsNone(last_read_cache)

        last_read = await self.env.db.get_oldest_last_read_in_group(group_id, session)
        last_read_cache = await self.env.cache.get_last_read_in_group_oldest(group_id)
        self.assertIsNotNone(last_read)
        self.assertIsNotNone(last_read_cache)
        self.assertEqual(last_read, last_read_cache)

    async def test_last_read_api_one_user(self):
        message = await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)

        last_read_a = await self.get_last_read_for_one_user(message["group_id"], BaseTest.USER_ID)
        last_read_b = await self.get_last_read_for_one_user(message["group_id"], BaseTest.OTHER_USER_ID)

        self.assertEqual(1, len(last_read_a["last_read_times"]))
        self.assertEqual(1, len(last_read_b["last_read_times"]))

        last_read_a = last_read_a["last_read_times"][0]["last_read_time"]
        last_read_b = last_read_b["last_read_times"][0]["last_read_time"]

        self.assertLess(self.long_ago, last_read_a)
        self.assertLess(self.long_ago, last_read_b)
        self.assertGreater(last_read_a, last_read_b)

    async def test_last_read_api_all_user(self):
        await self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)
        message = await self.send_1v1_message(user_id=BaseTest.OTHER_USER_ID, receiver_id=BaseTest.USER_ID)

        last_reads = await self.get_last_read_for_all_user(message["group_id"])

        self.assertEqual(2, len(last_reads["last_read_times"]))

        last_read_a, last_read_b = -1, -1
        for last in last_reads["last_read_times"]:
            if last["user_id"] == BaseTest.USER_ID:
                last_read_a = last["last_read_time"]
            elif last["user_id"] == BaseTest.OTHER_USER_ID:
                last_read_b = last["last_read_time"]

        self.assertLess(self.long_ago, last_read_a)
        self.assertLess(self.long_ago, last_read_b)

        # 8888 sent a message after 4444 so should have a more recent last_read
        self.assertGreater(last_read_b, last_read_a)

    async def _test_last_read_on_histories_response(self):
        message = await self.send_1v1_message()
        group_id = message["group_id"]

        histories = await self.histories_for(group_id)
        self.assertEqual(float, type(histories["last_read_time"]))

        # oldest last read is the other used; the message is unread for the other user,
        # so the oldest last read should be less than the message creation time
        self.assertGreater(message["created_at"], histories["last_read_time"])

