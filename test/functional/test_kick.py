from dinofw.rest.queries import CreateGroupQuery
from dinofw.rest.queries import GroupQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.rest.queries import UpdateUserGroupStats
from dinofw.utils.config import MessageTypes
from test.base import BaseTest, async_test
from test.functional.base_functional import BaseServerRestApi


class TestKickFromGroup(BaseServerRestApi):
    async def test_kicked_flag_is_updated(self):
        await self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        await self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        await self.update_kick_for_user(group_id=group_id, kicked=True, user_id=BaseTest.THIRD_USER_ID)
        await self.assert_kicked_for_user(kicked=True, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)

        # should still have the group, just that it's marked as kicked
        await self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

    async def test_kicked_user_does_not_get_msg_notifications(self):
        await self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        await self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        await self.update_kick_for_user(group_id=group_id, kicked=True, user_id=BaseTest.THIRD_USER_ID)
        await self.assert_kicked_for_user(kicked=True, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)

        self.assert_total_mqtt_sent_to(user_id=BaseTest.USER_ID, n_messages=0)
        self.assert_total_mqtt_sent_to(user_id=BaseTest.OTHER_USER_ID, n_messages=0)
        self.assert_total_mqtt_sent_to(user_id=BaseTest.THIRD_USER_ID, n_messages=0)

        await self.send_notification(group_id)

        self.assert_total_mqtt_sent_to(user_id=BaseTest.USER_ID, n_messages=1)        # sender should receive it
        self.assert_total_mqtt_sent_to(user_id=BaseTest.OTHER_USER_ID, n_messages=1)  # normal user receives it
        self.assert_total_mqtt_sent_to(user_id=BaseTest.THIRD_USER_ID, n_messages=0)  # kicked user doesn't receive it

    async def test_kicked_user_not_included_in_user_list(self):
        await self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        await self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        await self.update_kick_for_user(group_id=group_id, kicked=True, user_id=BaseTest.THIRD_USER_ID)
        await self.assert_kicked_for_user(kicked=True, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)

        group_info = await self.get_group_info(group_id=group_id, count_messages=False)
        user_in_group = {u["user_id"] for u in group_info["users"]}

        self.assertNotIn(BaseTest.THIRD_USER_ID, user_in_group)

    async def test_kicked_users_total_unread_count_is_not_updated(self):
        await self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        await self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        await self.update_kick_for_user(group_id=group_id, kicked=True, user_id=BaseTest.THIRD_USER_ID)
        await self.assert_kicked_for_user(kicked=True, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)

        await self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)
        await self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=0)
        await self.assert_total_unread_count(user_id=BaseTest.THIRD_USER_ID, unread_count=0)

        await self.send_message_to_group_from(group_id=group_id, user_id=BaseTest.USER_ID)

        await self.assert_total_unread_count(user_id=BaseTest.USER_ID, unread_count=0)        # sender doesn't increase
        await self.assert_total_unread_count(user_id=BaseTest.OTHER_USER_ID, unread_count=1)  # normal user increases
        await self.assert_total_unread_count(user_id=BaseTest.THIRD_USER_ID, unread_count=0)  # kicked user doesn't increase

    @BaseServerRestApi.init_db_session
    async def test_kick_resets_unread_count_mentions_pin_bookmark(self):
        session = self.env.db_session

        group_query = GroupQuery(
            per_page=1000,
            since=0,
            only_unread=False
        )
        user_groups = await self.env.rest.user.get_groups_for_user(BaseTest.THIRD_USER_ID, group_query, session)
        self.assertEqual(0, len(user_groups))

        create_query = CreateGroupQuery(
            users=[BaseTest.OTHER_USER_ID, BaseTest.THIRD_USER_ID],
            group_name="a new group",
            group_type=0
        )
        group = await self.env.rest.group.create_new_group(BaseTest.USER_ID, create_query, session)
        group_id = group.group_id

        # should have 1 group now
        user_groups = await self.env.rest.user.get_groups_for_user(BaseTest.THIRD_USER_ID, group_query, session)
        self.assertEqual(1, len(user_groups))

        send_query = SendMessageQuery(
            message_type=MessageTypes.MESSAGE,
            message_payload="some message",
            mention_user_ids=[BaseTest.THIRD_USER_ID]
        )
        await self.env.rest.message.send_message_to_group(group_id, BaseTest.USER_ID, send_query, session)

        # should increase unread by 1 when pinned and bookmarked
        update_query = UpdateUserGroupStats(bookmark=True, pin=True)
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.THIRD_USER_ID, update_query, session
        )

        # check the stats before getting kicked
        stats = await self.env.rest.group.get_user_group_stats(
            group_id, BaseTest.THIRD_USER_ID, session
        )
        self.assertTrue(stats.pin)
        self.assertTrue(stats.bookmark)
        self.assertEqual(1, stats.mentions)
        self.assertEqual(1, stats.unread)

        # kick the user
        update_query = UpdateUserGroupStats(kicked=True)
        await self.env.rest.group.update_user_group_stats(
            group_id, BaseTest.THIRD_USER_ID, update_query, session
        )

        # check the stats after kicked
        stats = await self.env.rest.group.get_user_group_stats(
            group_id, BaseTest.THIRD_USER_ID, session
        )
        self.assertTrue(stats.kicked)
        self.assertFalse(stats.pin)
        self.assertFalse(stats.bookmark)
        self.assertEqual(0, stats.mentions)
        self.assertEqual(0, stats.unread)

    async def test_join_group_resets_kicked_variable(self):
        await self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )
        await self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        await self.update_kick_for_user(group_id=group_id, kicked=True, user_id=BaseTest.THIRD_USER_ID)
        await self.assert_kicked_for_user(kicked=True, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)

        await self.user_joins_group(group_id, user_id=BaseTest.THIRD_USER_ID)
        await self.assert_kicked_for_user(kicked=False, group_id=group_id, user_id=BaseTest.THIRD_USER_ID)
