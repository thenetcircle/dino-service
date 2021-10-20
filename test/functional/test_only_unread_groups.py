from test.base import BaseTest
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

    def test_hide_only_unread(self):
        self.assert_groups_for_user(0)
        group_message = self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=8888)
        group_id = group_message["group"]["group_id"]

        self.send_1v1_message(user_id=BaseTest.USER_ID, receiver_id=9999)
        self.send_1v1_message(user_id=8888, receiver_id=BaseTest.USER_ID)

        groups = self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(1, len(groups))

        self.update_hide_group_for(group_id, hide=True, user_id=BaseTest.USER_ID)
        self.assert_hidden_for_user(True, group_id, user_id=BaseTest.USER_ID)

        groups = self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(0, len(groups))

        self.histories_for(group_id, user_id=BaseTest.USER_ID)
        self.assert_hidden_for_user(False, group_id, user_id=BaseTest.USER_ID)

        groups = self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=False)
        self.assertEqual(2, len(groups))

        groups = self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(0, len(groups))

        self.send_1v1_message(user_id=8888, receiver_id=BaseTest.USER_ID)

        self.assert_hidden_for_user(False, group_id, user_id=BaseTest.USER_ID)
        groups = self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(1, len(groups))

        self.update_hide_group_for(group_id, hide=True, user_id=BaseTest.USER_ID)
        self.assert_hidden_for_user(True, group_id, user_id=BaseTest.USER_ID)

        groups = self.groups_for_user(user_id=BaseTest.USER_ID, count_unread=True, only_unread=True)
        self.assertEqual(0, len(groups))
