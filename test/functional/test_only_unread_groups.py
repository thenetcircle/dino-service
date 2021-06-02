from dinofw.rest.queries import AbstractQuery
from test.base import BaseTest
import arrow
from test.functional.base_functional import BaseServerRestApi


class TestOnlyUnreadGroups(BaseServerRestApi):
    def test_only_unread(self):
        self.assert_groups_for_user(0)
        self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=8888)
        self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=9999)
        self.send_1v1_message(user_id=9999, receiver_id=BaseTest.USER_ID)

        groups = self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False, only_unread=False)
        self.assertEqual(2, len(groups))

        groups = self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=False, only_unread=True)
        self.assertEqual(1, len(groups))

        groups = self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=False)
        self.assertEqual(2, len(groups))

        groups = self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(1, len(groups))
