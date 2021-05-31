import arrow

from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestBookmark(BaseServerRestApi):
    def test_removing_bookmark_resets_unread_count(self):
        self.assert_groups_for_user(0)
        message = self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )
        group_id = message["group_id"]

        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(1, stats["unread"])
        self.assertEqual(False, stats["bookmark"])

        self.bookmark_group(group_id, bookmark=True, user_id=BaseTest.OTHER_USER_ID)
        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(1, stats["unread"])
        self.assertEqual(True, stats["bookmark"])

        self.bookmark_group(group_id, bookmark=False, user_id=BaseTest.OTHER_USER_ID)
        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(0, stats["unread"])
        self.assertEqual(False, stats["bookmark"])

    def test_removing_bookmark_resets_highlight(self):
        self.assert_groups_for_user(0)
        message = self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )
        group_id = message["group_id"]

        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(1, stats["unread"])
        self.assertEqual(self.long_ago, stats["highlight_time"])

        # highlight the group
        self.highlight_group_for_user(
            group_id,
            user_id=BaseTest.OTHER_USER_ID,
            highlight_time=arrow.utcnow().float_timestamp
        )

        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(1, stats["unread"])
        self.assertEqual(False, stats["bookmark"])
        self.assertLess(self.long_ago, stats["highlight_time"])

        # add the bookmark
        self.bookmark_group(group_id, bookmark=True, user_id=BaseTest.OTHER_USER_ID)

        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(1, stats["unread"])
        self.assertEqual(True, stats["bookmark"])
        self.assertLess(self.long_ago, stats["highlight_time"])

        receiver_stats = self.groups_for_user(BaseTest.USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(stats["highlight_time"], receiver_stats["receiver_highlight_time"])
        self.assertLess(self.long_ago, receiver_stats["receiver_highlight_time"])

        # remove the bookmark should reset highlight time and unread count
        self.bookmark_group(group_id, bookmark=False, user_id=BaseTest.OTHER_USER_ID)

        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(0, stats["unread"])
        self.assertEqual(False, stats["bookmark"])
        self.assertEqual(self.long_ago, stats["highlight_time"])

        receiver_stats = self.groups_for_user(BaseTest.USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(stats["highlight_time"], receiver_stats["receiver_highlight_time"])
        self.assertEqual(self.long_ago, receiver_stats["receiver_highlight_time"])

    def test_removing_bookmark_resets_last_read(self):
        self.assert_groups_for_user(0)
        message = self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )
        group_id = message["group_id"]

        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(1, stats["unread"])
        self.assertEqual(self.long_ago, stats["highlight_time"])

        self.histories_for(group_id, user_id=BaseTest.OTHER_USER_ID)

        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        last_read_time = stats["last_read_time"]

        # add the bookmark
        self.bookmark_group(group_id, bookmark=True, user_id=BaseTest.OTHER_USER_ID)

        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(True, stats["bookmark"])
        self.assertEqual(last_read_time, stats["last_read_time"])

        # send another message
        self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        # last read time should be same as before
        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(True, stats["bookmark"])
        self.assertEqual(last_read_time, stats["last_read_time"])

        # remove the bookmark should reset last read time
        self.bookmark_group(group_id, bookmark=False, user_id=BaseTest.OTHER_USER_ID)

        stats = self.groups_for_user(BaseTest.OTHER_USER_ID, count_unread=True)[0]["stats"]
        self.assertEqual(False, stats["bookmark"])
        self.assertLess(last_read_time, stats["last_read_time"])
