import time

from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestServerRestApi(BaseServerRestApi):
    def test_get_groups_for_user_before_joining(self):
        self.assert_groups_for_user(0)

    def test_get_groups_for_user_after_joining(self):
        self.create_and_join_group()
        self.assert_groups_for_user(1)

    def test_leaving_a_group(self):
        self.assert_groups_for_user(0)

        group_id = self.create_and_join_group()
        self.assert_groups_for_user(1)

        self.user_leaves_group(group_id)
        self.assert_groups_for_user(0)

    def test_another_user_joins_group(self):
        self.assert_groups_for_user(0, user_id=BaseTest.USER_ID)
        self.assert_groups_for_user(0, user_id=BaseTest.OTHER_USER_ID)

        # first user joins, check that other user isn't in any groups
        group_id = self.create_and_join_group()
        self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        self.assert_groups_for_user(0, user_id=BaseTest.OTHER_USER_ID)

        # other user also joins, check that both are in a group now
        self.user_joins_group(group_id, user_id=BaseTest.OTHER_USER_ID)
        self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)

    def test_users_in_group(self):
        self.assert_groups_for_user(0, user_id=BaseTest.USER_ID)
        self.assert_groups_for_user(0, user_id=BaseTest.OTHER_USER_ID)

        # first user joins, check that other user isn't in any groups
        group_id = self.create_and_join_group()
        self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        self.assert_groups_for_user(0, user_id=BaseTest.OTHER_USER_ID)

        # other user also joins, check that both are in a group now
        self.user_joins_group(group_id, user_id=BaseTest.OTHER_USER_ID)
        self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)

    def test_update_user_statistics_in_group(self):
        group_id = self.create_and_join_group()

        now_ts = self.update_user_stats_to_now(group_id, BaseTest.USER_ID)
        user_stats = self.get_user_stats(group_id, BaseTest.USER_ID)

        self.assertEqual(group_id, user_stats["group_id"])
        self.assertEqual(BaseTest.USER_ID, user_stats["user_id"])
        self.assertEqual(now_ts, user_stats["last_read_time"])

    def test_group_unhidden_on_new_message_for_all_users(self):
        # both users join a new group
        group_id = self.create_and_join_group(BaseTest.USER_ID)
        self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)

        # the group should not be hidden for either user at this time
        self.assert_hidden_for_user(False, group_id, BaseTest.USER_ID)
        self.assert_hidden_for_user(False, group_id, BaseTest.OTHER_USER_ID)

        # both users should have the group in the list
        self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)

        # hide the group for the other user
        self.update_hide_group_for(group_id, True, BaseTest.OTHER_USER_ID)

        # make sure the group is hidden for the other user
        self.assert_hidden_for_user(False, group_id, BaseTest.USER_ID)
        self.assert_hidden_for_user(True, group_id, BaseTest.OTHER_USER_ID)

        # other user doesn't have any groups since he hid it
        self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        self.assert_groups_for_user(0, user_id=BaseTest.OTHER_USER_ID)

        # sending a message should un-hide the group for all users in it
        self.send_message_to_group_from(group_id, BaseTest.USER_ID)

        # should not be hidden anymore for any user
        self.assert_hidden_for_user(False, group_id, BaseTest.USER_ID)
        self.assert_hidden_for_user(False, group_id, BaseTest.OTHER_USER_ID)

        # both users have 1 group now since none is hidden anymore
        self.assert_groups_for_user(1, user_id=BaseTest.USER_ID)
        self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)

    def test_one_user_deletes_some_history(self):
        # both users join a new group
        group_id = self.create_and_join_group(BaseTest.USER_ID)
        self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)

        self.assert_messages_in_group(group_id, user_id=BaseTest.USER_ID, amount=0)
        self.assert_messages_in_group(group_id, user_id=BaseTest.OTHER_USER_ID, amount=0)

        # each user sends 4 messages each, then we delete some of them for one user
        messages_to_send_each = 4

        self.send_message_to_group_from(
            group_id, user_id=BaseTest.USER_ID, amount=messages_to_send_each, delay=10
        )
        messages = self.send_message_to_group_from(
            group_id, user_id=BaseTest.OTHER_USER_ID, amount=messages_to_send_each, delay=10
        )

        # first user deletes the first 5 messages in the group
        self.update_delete_before(group_id, delete_before=messages[0]["created_at"], user_id=BaseTest.USER_ID)

        # first user should have 3, since we deleted everything before the other user's
        # first message (including that first message); second user should have all 8
        # since he/she didn't delete anything
        self.assert_messages_in_group(group_id, user_id=BaseTest.USER_ID, amount=messages_to_send_each - 1)
        self.assert_messages_in_group(group_id, user_id=BaseTest.OTHER_USER_ID, amount=messages_to_send_each * 2)

    def test_joining_a_group_changes_last_update_time(self):
        group_id = self.create_and_join_group(BaseTest.USER_ID)
        group = self.get_group(group_id)
        last_update_time = group["updated_at"]

        # send a message
        self.send_message_to_group_from(group_id, user_id=BaseTest.USER_ID, delay=10)

        # update time should not have changed form a new message
        group = self.get_group(group_id)
        self.assertEqual(group["updated_at"], last_update_time)

        # should change update time
        self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)

        # update time should now have changed
        group = self.get_group(group_id)
        self.assertNotEqual(group["updated_at"], last_update_time)

    def test_total_unread_count_changes_when_user_read_time_changes(self):
        group_id1 = self.create_and_join_group(BaseTest.USER_ID)

        self.user_joins_group(group_id1, BaseTest.OTHER_USER_ID)
        self.send_message_to_group_from(group_id1, user_id=BaseTest.USER_ID, amount=10, delay=10)

        self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)
        self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=10)

        group_id2 = self.create_and_join_group(BaseTest.USER_ID)

        self.user_joins_group(group_id2, BaseTest.OTHER_USER_ID)
        self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID, amount=10, delay=10)

        self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)
        self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=20)

        # sending a message should mark the group as "read"
        self.send_message_to_group_from(group_id2, user_id=BaseTest.OTHER_USER_ID)

        self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=1)
        self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=10)

        # first user should now have 2 unread
        self.send_message_to_group_from(group_id2, user_id=BaseTest.OTHER_USER_ID)

        self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=2)
        self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=10)

        # first user should now have 3 unread
        self.send_message_to_group_from(group_id1, user_id=BaseTest.OTHER_USER_ID)

        self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=3)
        self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=0)

    def test_pin_group_changes_ordering(self):
        group_id1 = self.create_and_join_group(BaseTest.USER_ID)
        group_id2 = self.create_and_join_group(BaseTest.USER_ID)

        self.send_message_to_group_from(group_id1, user_id=BaseTest.USER_ID)
        time.sleep(0.1)

        # group 2 should now be on top
        self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID)
        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(groups[0]["group_id"], group_id2)
        self.assertEqual(groups[1]["group_id"], group_id1)

        # should be in the other order after pinning
        self.pin_group_for(group_id1, BaseTest.USER_ID)
        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(groups[0]["group_id"], group_id1)
        self.assertEqual(groups[1]["group_id"], group_id2)

        # should not change order since group 1 is pinned
        self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID)
        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(groups[0]["group_id"], group_id1)
        self.assertEqual(groups[1]["group_id"], group_id2)

        # after unpinning the group with the latest message should be first
        self.unpin_group_for(group_id1, BaseTest.USER_ID)
        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(groups[0]["group_id"], group_id2)
        self.assertEqual(groups[1]["group_id"], group_id1)
