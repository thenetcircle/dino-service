from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestLastReadTime(BaseServerRestApi):
    def test_get_oldest_last_read_in_group(self):
        session = self.env.session_maker()

        message = self.send_1v1_message()
        group_id = message["group_id"]

        last_read_cache = self.env.cache.get_last_read_in_group_oldest(group_id)
        self.assertIsNone(last_read_cache)

        last_read = self.env.db.get_oldest_last_read_in_group(group_id, session)
        last_read_cache = self.env.cache.get_last_read_in_group_oldest(group_id)
        self.assertIsNotNone(last_read)
        self.assertIsNotNone(last_read_cache)
        self.assertEqual(last_read, last_read_cache)

    def test_last_read_api_one_user(self):
        message = self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=BaseTest.OTHER_USER_ID)

        last_reads_4444 = self.get_last_read_for_one_user(message["group_id"], BaseTest.USER_ID)
        last_reads_8888 = self.get_last_read_for_one_user(message["group_id"], BaseTest.OTHER_USER_ID)

        self.assertEqual(1, len(last_reads_4444["last_read_times"]))
        self.assertEqual(1, len(last_reads_8888["last_read_times"]))

        last_read_4444 = last_reads_4444["last_read_times"][0]["last_read_time"]
        last_read_8888 = last_reads_8888["last_read_times"][0]["last_read_time"]

        self.assertLess(self.long_ago, last_read_4444)
        self.assertLess(self.long_ago, last_read_8888)
        self.assertGreater(last_read_4444, last_read_8888)

    def _test_last_read_on_histories_response(self):
        message = self.send_1v1_message()
        group_id = message["group_id"]

        histories = self.histories_for(group_id)
        self.assertEqual(float, type(histories["last_read_time"]))

        # oldest last read is the other used; the message is unread for the other user,
        # so the oldest last read should be less than the message creation time
        self.assertGreater(message["created_at"], histories["last_read_time"])

