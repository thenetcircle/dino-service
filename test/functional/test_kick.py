from dinofw.rest.queries import SendMessageQuery
from dinofw.utils.config import MessageTypes
from test.base import BaseTest, async_test
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

    @async_test
    async def test_kick_resets_unread_count_mentions_pin_bookmark(self):
        session = self.env.session_maker()
        self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        group_id = self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        send_query = SendMessageQuery(
            message_type=MessageTypes.MESSAGE,
            message_payload="some message",
            mention_user_ids=[BaseTest.THIRD_USER_ID]
        )
        await self.env.rest.message.send_message_to_group(group_id, BaseTest.USER_ID, send_query, session)

        self.bookmark_group(group_id, bookmark=True, user_id=BaseTest.THIRD_USER_ID)
        self.pin_group_for(group_id, user_id=BaseTest.THIRD_USER_ID)

        stats = self.get_user_stats(group_id, BaseTest.THIRD_USER_ID)["stats"]
        self.assert_total_unread_count(BaseTest.THIRD_USER_ID, 1)
        self.assertTrue(stats["pin"])
        self.assertTrue(stats["bookmark"])
        self.assertEqual(1, stats["mentions"])
        self.assertEqual(1, stats["unread"])

        self.update_kick_for_user(group_id=group_id, kicked=True, user_id=BaseTest.THIRD_USER_ID)
        self.assert_kicked_for_user(kicked=True, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)

        stats = self.get_user_stats(group_id, BaseTest.THIRD_USER_ID)["stats"]
        self.assert_total_unread_count(BaseTest.THIRD_USER_ID, 0)
        self.assertFalse(stats["pin"])
        self.assertFalse(stats["bookmark"])
        self.assertEqual(0, stats["mentions"])
        self.assertEqual(0, stats["unread"])

    def test_join_group_resets_kicked_variable(self):
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

        self.user_joins_group(group_id, user_id=BaseTest.THIRD_USER_ID)
        self.assert_kicked_for_user(kicked=False, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)
