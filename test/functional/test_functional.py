from dinofw.utils.config import MessageTypes
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
        last_update_time = group["group"]["updated_at"]

        # update time should not have changed form a new message
        self.send_message_to_group_from(group_id, user_id=BaseTest.USER_ID, delay=10)
        group = self.get_group(group_id)
        self.assertEqual(group["group"]["updated_at"], last_update_time)

        # update time should now have changed
        self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)
        group = self.get_group(group_id)
        self.assertNotEqual(group["group"]["updated_at"], last_update_time)

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

        self.send_message_to_group_from(group_id1, user_id=BaseTest.USER_ID, delay=50)

        # group 2 should now be on top
        self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_id2, group_id1)

        # should be in the other order after pinning
        self.pin_group_for(group_id1, BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_id1, group_id2)

        # should not change order since group 1 is pinned
        self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_id1, group_id2)

        # after unpinning the group with the latest message should be first
        self.unpin_group_for(group_id1, BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_id2, group_id1)

    def test_last_read_updated_in_history_api(self):
        group_id = self.create_and_join_group(BaseTest.USER_ID)
        self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)

        histories = self.histories_for(group_id, BaseTest.USER_ID)
        last_read_user_1_before = self.last_read_in_histories_for(histories, BaseTest.USER_ID)
        last_read_user_2_before = self.last_read_in_histories_for(histories, BaseTest.OTHER_USER_ID)

        self.send_message_to_group_from(group_id, user_id=BaseTest.USER_ID, delay=50)

        histories = self.histories_for(group_id, BaseTest.USER_ID)
        last_read_user_1_after = self.last_read_in_histories_for(histories, BaseTest.USER_ID)
        last_read_user_2_after = self.last_read_in_histories_for(histories, BaseTest.OTHER_USER_ID)

        self.assertNotEqual(last_read_user_1_before, last_read_user_1_after)
        self.assertEqual(last_read_user_2_before, last_read_user_2_after)

    def test_last_read_removed_on_leave(self):
        group_id = self.create_and_join_group(BaseTest.USER_ID)
        self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)

        histories = self.histories_for(group_id, BaseTest.USER_ID)
        self.assert_in_histories(BaseTest.USER_ID, histories, is_in=True)
        self.assert_in_histories(BaseTest.OTHER_USER_ID, histories, is_in=True)

        self.user_leaves_group(group_id, BaseTest.OTHER_USER_ID)
        self.send_message_to_group_from(group_id, user_id=BaseTest.USER_ID)

        histories = self.histories_for(group_id, BaseTest.USER_ID)
        self.assert_in_histories(BaseTest.USER_ID, histories, is_in=True)
        self.assert_in_histories(BaseTest.OTHER_USER_ID, histories, is_in=False)

    def test_group_exists_when_leaving(self):
        self.user_leaves_group(BaseTest.GROUP_ID)

        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(0, len(groups))

        group_id = self.create_and_join_group(BaseTest.USER_ID)
        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(1, len(groups))

        self.user_leaves_group(group_id)
        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(0, len(groups))

    def test_highlight_group_for_other_user(self):
        group_id1 = self.create_and_join_group(BaseTest.USER_ID)
        group_id2 = self.create_and_join_group(BaseTest.USER_ID)

        self.user_joins_group(group_id1, BaseTest.OTHER_USER_ID)
        self.user_joins_group(group_id2, BaseTest.OTHER_USER_ID)

        self.send_message_to_group_from(group_id1, user_id=BaseTest.USER_ID, delay=50)
        self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_id2, group_id1)

        self.highlight_group_for_user(group_id1, BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_id1, group_id2)

    def test_highlight_makes_group_unhidden(self):
        group_id = self.create_and_join_group(BaseTest.USER_ID)
        self.user_joins_group(group_id, BaseTest.OTHER_USER_ID)

        # just joined a group, should have one
        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(1, len(groups))

        # after hiding we should not have any groups anymore
        self.update_hide_group_for(group_id, hide=True, user_id=BaseTest.USER_ID)
        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(0, len(groups))

        # make sure it becomes unhidden if highlighted by someone
        self.highlight_group_for_user(group_id, BaseTest.USER_ID)
        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(1, len(groups))

    def _test_delete_highlight_changes_order(self):
        # TODO: don't delete hightlight, just call the history api for the group

        group_id1 = self.create_and_join_group(BaseTest.USER_ID)
        group_id2 = self.create_and_join_group(BaseTest.USER_ID)

        self.user_joins_group(group_id1, BaseTest.OTHER_USER_ID)
        self.user_joins_group(group_id2, BaseTest.OTHER_USER_ID)

        self.send_message_to_group_from(group_id1, user_id=BaseTest.USER_ID, delay=100)
        self.send_message_to_group_from(group_id2, user_id=BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_id2, group_id1)

        self.highlight_group_for_user(group_id1, BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_id1, group_id2)

        # back to normal
        self.delete_highlight_group_for_user(group_id1, BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_id2, group_id1)

    def _test_highlight_ordered_higher_than_pin(self):
        # TODO: don't delete hightlight, just call the history api for the group

        group_1 = self.create_and_join_group(BaseTest.USER_ID)
        group_2 = self.create_and_join_group(BaseTest.USER_ID)
        group_3 = self.create_and_join_group(BaseTest.USER_ID)

        # first send a message to each group with a short delay
        for group_id in [group_1, group_2, group_3]:
            self.send_message_to_group_from(group_id, user_id=BaseTest.USER_ID, delay=50)

        # last group to receive a message should be on top
        self.assert_order_of_groups(BaseTest.USER_ID, group_3, group_2, group_1)

        # pinned a group should put it at the top
        self.pin_group_for(group_2, user_id=BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_2, group_3, group_1)

        # highlight has priority over pinning, so group 1 should be above group 2 now
        self.highlight_group_for_user(group_1, user_id=BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_1, group_2, group_3)

        # sending a message to group 3 should not change anything, since 1 and 2 are highlighted and pinned respectively
        self.send_message_to_group_from(group_3, user_id=BaseTest.USER_ID, delay=50)
        self.assert_order_of_groups(BaseTest.USER_ID, group_1, group_2, group_3)

        # group 2 and 3 are pinned, but 3 has more recent message now
        self.pin_group_for(group_3, user_id=BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_1, group_3, group_2)

        # group 1 has now the older message and not highlighted so should be at the bottom
        self.delete_highlight_group_for_user(group_1, BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_3, group_2, group_1)

        # group 2 should be on top after highlighting it
        self.highlight_group_for_user(group_2, user_id=BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_2, group_3, group_1)

        # group 1 has a more recent highlight than group 2
        self.highlight_group_for_user(group_1, user_id=BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_1, group_2, group_3)

        # pinning group 2 should not change anything since highlight has priority over pinning
        self.pin_group_for(group_2, user_id=BaseTest.USER_ID)
        self.assert_order_of_groups(BaseTest.USER_ID, group_1, group_2, group_3)

    def test_change_group_name(self):
        self.create_and_join_group(BaseTest.USER_ID)

        groups = self.groups_for_user(BaseTest.USER_ID)
        original_name = groups[0]["group"]["name"]
        self.assertIsNotNone(original_name)

        new_name = "new test name for group"
        self.edit_group(groups[0]["group"]["group_id"], name=new_name)

        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(groups[0]["group"]["name"], new_name)
        self.assertNotEqual(original_name, new_name)

    def test_change_group_owner(self):
        self.create_and_join_group(BaseTest.USER_ID)

        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(BaseTest.USER_ID, groups[0]["group"]["owner_id"])

        self.edit_group(groups[0]["group"]["group_id"], owner=BaseTest.OTHER_USER_ID)
        groups = self.groups_for_user(BaseTest.USER_ID)
        self.assertEqual(BaseTest.OTHER_USER_ID, groups[0]["group"]["owner_id"])

    def test_attachments(self):
        message = self.send_1v1_message(message_type=MessageTypes.IMAGE)
        self.assertEqual(MessageTypes.IMAGE, int(message["message_type"]))
        self.assertIsNone(message["message_payload"])

        info = self.get_1v1_group_info()
        group_id = info["group"]["group_id"]
        history = self.histories_for(group_id)

        self.assertEqual(1, len(history["messages"]))
        self.assertEqual(0, len(history["attachments"]))
        self.assertEqual(message["message_id"], history["messages"][0]["message_id"])

        attachment = self.create_attachment(message["message_id"])
        self.assertEqual(attachment["message_id"], message["message_id"])

        history = self.histories_for(group_id)

        self.assertEqual(1, len(history["messages"]))
        self.assertEqual(1, len(history["attachments"]))
        self.assertEqual(message["message_id"], history["messages"][0]["message_id"])
        self.assertEqual(message["message_id"], history["attachments"][0]["message_id"])
