from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestKickFromGroup(BaseServerRestApi):
    def test_kicked_flag_is_updated(self):
        self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        self.update_kick_for_user(group_id=group_id, kicked=True, user_id=BaseTest.THIRD_USER_ID)
        self.assert_kicked_for_user(kicked=True, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)

        # should still have the group, just that it's marked as kicked
        self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

    def test_kicked_user_does_not_get_msg_notifications(self):
        self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        self.update_kick_for_user(group_id=group_id, kicked=True, user_id=BaseTest.THIRD_USER_ID)
        self.assert_kicked_for_user(kicked=True, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)

        self.assert_total_mqtt_sent_to(user_id=BaseTest.USER_ID, n_messages=0)
        self.assert_total_mqtt_sent_to(user_id=BaseTest.OTHER_USER_ID, n_messages=0)
        self.assert_total_mqtt_sent_to(user_id=BaseTest.THIRD_USER_ID, n_messages=0)

        self.send_notification(group_id)

        self.assert_total_mqtt_sent_to(user_id=BaseTest.USER_ID, n_messages=1)        # sender should receive it
        self.assert_total_mqtt_sent_to(user_id=BaseTest.OTHER_USER_ID, n_messages=1)  # normal user receives it
        self.assert_total_mqtt_sent_to(user_id=BaseTest.THIRD_USER_ID, n_messages=0)  # kicked user doesn't receive it

    def test_kicked_user_not_included_in_user_list(self):
        self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        self.update_kick_for_user(group_id=group_id, kicked=True, user_id=BaseTest.THIRD_USER_ID)
        self.assert_kicked_for_user(kicked=True, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)

        group_info = self.get_group_info(group_id=group_id, count_messages=False)
        user_in_group = {u["user_id"] for u in group_info["users"]}

        self.assertNotIn(BaseTest.THIRD_USER_ID, user_in_group)

    def test_kicked_users_total_unread_count_is_not_updated(self):
        self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        self.update_kick_for_user(group_id=group_id, kicked=True, user_id=BaseTest.THIRD_USER_ID)
        self.assert_kicked_for_user(kicked=True, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)

        self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)
        self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=0)
        self.assert_total_unread_count(user_id=BaseTest.THIRD_USER_ID, unread_count=0)

        self.send_message_to_group_from(group_id=group_id, user_id=BaseTest.USER_ID)

        self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)        # sender doesn't increase
        self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=1)  # normal user increases
        self.assert_total_unread_count(user_id=BaseTest.THIRD_USER_ID, unread_count=0)  # kicked user doesn't increase
