from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestReceiverStats(BaseServerRestApi):
    def test_receiver_stats_is_none(self):
        self.assert_groups_for_user(0)
        self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        stats = self.groups_for_user(
            BaseTest.USER_ID,
            count_unread=False,
            receiver_stats=False
        )[0]["stats"]

        self.assertEqual(None, stats["receiver_delete_before"])
        self.assertEqual(None, stats["receiver_hide"])
        self.assertEqual(None, stats["receiver_deleted"])
        self.assertEqual(-1, stats["unread"])
        self.assertEqual(-1, stats["receiver_unread"])

    def test_receiver_stats_is_not_none(self):
        self.assert_groups_for_user(0)
        self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        stats = self.groups_for_user(
            BaseTest.USER_ID,
            count_unread=False,
            receiver_stats=True
        )[0]["stats"]

        self.assertLess(self.long_ago, stats["receiver_delete_before"])
        self.assertEqual(False, stats["receiver_hide"])
        self.assertEqual(False, stats["receiver_deleted"])
        self.assertEqual(-1, stats["unread"])
        self.assertEqual(1, stats["receiver_unread"])

    def test_unread(self):
        self.assert_groups_for_user(0)
        self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        stats = self.groups_for_user(
            BaseTest.USER_ID,
            count_unread=True,
            receiver_stats=True
        )[0]["stats"]

        self.assertLess(self.long_ago, stats["receiver_delete_before"])
        self.assertEqual(False, stats["receiver_hide"])
        self.assertEqual(False, stats["receiver_deleted"])
        self.assertEqual(0, stats["unread"])
        self.assertEqual(1, stats["receiver_unread"])

    def test_unread_no_receiver_stats(self):
        self.assert_groups_for_user(0)
        self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        stats = self.groups_for_user(
            BaseTest.USER_ID,
            count_unread=True,
            receiver_stats=False
        )[0]["stats"]

        self.assertEqual(None, stats["receiver_delete_before"])
        self.assertEqual(None, stats["receiver_hide"])
        self.assertEqual(None, stats["receiver_deleted"])
        self.assertEqual(0, stats["unread"])
        self.assertEqual(-1, stats["receiver_unread"])
