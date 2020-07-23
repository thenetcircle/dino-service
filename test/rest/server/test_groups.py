from dinofw.rest.server.groups import GroupResource
from dinofw.rest.server.message import MessageResource
from dinofw.rest.server.models import CreateGroupQuery, GroupUsers, Group, MessageQuery, SendMessageQuery
from test.base import async_test, BaseTest
from test.mocks import FakeEnv, FakeStorage


class TestGroupResource(BaseTest):
    def setUp(self) -> None:
        env = FakeEnv()

        self.group = GroupResource(env)
        self.message = MessageResource(env)

    @async_test
    async def test_create_new_group(self):
        group_name = "new group name"
        query = CreateGroupQuery(
            group_name=group_name,
            group_type=0,
            users=[BaseTest.USER_ID],
        )

        self.assertEqual(0, len(self.group.env.storage.action_log))

        group = await self.group.create_new_group(
            user_id=BaseTest.USER_ID,
            query=query,
            db=None  # noqa
        )

        self.assertIsNotNone(group)
        self.assertEqual(type(group), Group)
        self.assertEqual(group_name, group.name)
        self.assertEqual(1, len(self.group.env.storage.action_log))
        self.assertEqual(1, len(self.group.env.storage.action_log[group.group_id]))

    @async_test
    async def test_get_users_in_group(self):
        query = CreateGroupQuery(
            group_name="a group name",
            group_type=0,
            users=[BaseTest.USER_ID],
        )
        group = await self.group.create_new_group(
            user_id=BaseTest.USER_ID,
            query=query,
            db=None  # noqa
        )

        group_users = await self.group.get_users_in_group(
            group_id=group.group_id,
            db=None  # noqa
        )

        self.assertIsNotNone(group_users)
        self.assertEqual(type(group_users), GroupUsers)
        self.assertEqual(1, group_users.user_count)
        self.assertEqual(BaseTest.USER_ID, group_users.users[0].user_id)
        self.assertEqual(BaseTest.USER_ID, group_users.owner_id)

    @async_test
    async def test_get_group(self):
        group = await self.group.get_group(
            group_id=BaseTest.GROUP_ID,
            db=None  # noqa
        )

        self.assertIsNone(group)

        query = CreateGroupQuery(
            group_name="some group name",
            group_type=0,
            users=[BaseTest.USER_ID],
        )
        group = await self.group.create_new_group(
            user_id=BaseTest.USER_ID,
            query=query,
            db=None  # noqa
        )

        self.assertIsNotNone(group)
        self.assertEqual(type(group), Group)

        group = await self.group.get_group(
            group_id=group.group_id,
            db=None  # noqa
        )

        self.assertIsNotNone(group)
        self.assertEqual(type(group), Group)

    @async_test
    async def test_histories(self):
        message_query = MessageQuery(per_page=10)
        create_query = CreateGroupQuery(
            group_name="some group name",
            group_type=0,
            users=[BaseTest.USER_ID],
        )
        send_query = SendMessageQuery(
            message_payload="some text",
            message_type="text"
        )

        histories = await self.group.histories(BaseTest.GROUP_ID, message_query)

        self.assertIsNotNone(histories)
        self.assertEqual(0, len(histories.messages))
        self.assertEqual(0, len(histories.action_logs))

        # create a new group
        group = await self.group.create_new_group(BaseTest.USER_ID, create_query, None)  # noqa

        # send message and get histories
        await self.message.save_new_message(group.group_id, BaseTest.USER_ID, send_query, None)  # noqa
        histories = await self.group.histories(group.group_id, message_query)

        # one join event and one message
        self.assertEqual(1, len(histories.messages))
        self.assertEqual(1, len(histories.action_logs))

        # send another message
        await self.message.save_new_message(group.group_id, BaseTest.USER_ID, send_query, None)  # noqa
        histories = await self.group.histories(group.group_id, message_query)

        # now we should have two messages but still only one join event
        self.assertEqual(2, len(histories.messages))
        self.assertEqual(1, len(histories.action_logs))

    @async_test
    async def test_get_user_group_stats(self):
        create_query = CreateGroupQuery(
            group_name="some group name",
            group_type=0,
            users=[BaseTest.USER_ID],
        )
        send_query = SendMessageQuery(
            message_payload="some text",
            message_type="text"
        )

        # group doesn't exist
        stats = await self.group.get_user_group_stats(BaseTest.GROUP_ID, BaseTest.USER_ID, None)  # noqa
        self.assertIsNone(stats)

        # create a new group
        group = await self.group.create_new_group(BaseTest.USER_ID, create_query, None)  # noqa
        stats = await self.group.get_user_group_stats(group.group_id, BaseTest.USER_ID, None)  # noqa
        self.assertEqual(0, stats.message_amount)
        self.assertEqual(0, stats.unread_amount)

        # send a message, should have 0 unread since we sent it
        await self.message.save_new_message(group.group_id, BaseTest.USER_ID, send_query, None)  # noqa
        stats = await self.group.get_user_group_stats(group.group_id, BaseTest.USER_ID, None)  # noqa
        self.assertEqual(1, stats.message_amount)
        self.assertEqual(0, stats.unread_amount)

        # another user sends a message, should have 1 unread now
        await self.message.save_new_message(group.group_id, BaseTest.OTHER_USER_ID, send_query, None)  # noqa
        stats = await self.group.get_user_group_stats(group.group_id, BaseTest.USER_ID, None)  # noqa
        self.assertEqual(2, stats.message_amount)
        self.assertEqual(1, stats.unread_amount)

    @async_test
    async def test_join_group(self):
        create_query = CreateGroupQuery(
            group_name="some group name",
            group_type=0,
            users=[BaseTest.USER_ID],
        )

        # create a new group
        group = await self.group.create_new_group(BaseTest.USER_ID, create_query, None)  # noqa

        # check we only have one user in the group, the creator
        group_users = await self.group.get_users_in_group(group.group_id, None)  # noqa
        self.assertIsNotNone(group_users)
        self.assertEqual(1, group_users.user_count)
        self.assertEqual(1, len(group_users.users))
        self.assertTrue(any((g.user_id == BaseTest.USER_ID for g in group_users.users)))

        # other user joins it
        log = await self.group.join_group(group.group_id, BaseTest.OTHER_USER_ID, None)  # noqa
        self.assertIsNotNone(log)
        self.assertEqual(BaseTest.OTHER_USER_ID, log.user_id)
        self.assertEqual(FakeStorage.ACTION_TYPE_JOIN, log.action_type)

        # check the other user is now in the group as well
        group_users = await self.group.get_users_in_group(group.group_id, None)  # noqa
        self.assertIsNotNone(group_users)
        self.assertEqual(2, group_users.user_count)
        self.assertEqual(2, len(group_users.users))
        self.assertTrue(any((g.user_id == BaseTest.USER_ID for g in group_users.users)))
        self.assertTrue(any((g.user_id == BaseTest.OTHER_USER_ID for g in group_users.users)))

    @async_test
    async def test_leave_group(self):
        create_query = CreateGroupQuery(
            group_name="some group name",
            group_type=0,
            users=[BaseTest.USER_ID],
        )

        # group doesn't exist yet
        log = await self.group.leave_group(BaseTest.GROUP_ID, BaseTest.USER_ID, None)  # noqa
        self.assertIsNone(log)

        # create a new group
        group = await self.group.create_new_group(BaseTest.USER_ID, create_query, None)  # noqa

        # check we only have one user in the group, the creator
        group_users = await self.group.get_users_in_group(group.group_id, None)  # noqa
        self.assertIsNotNone(group_users)
        self.assertEqual(1, group_users.user_count)

        # leave the group
        log = await self.group.leave_group(group.group_id, BaseTest.USER_ID, None)  # noqa
        self.assertIsNotNone(log)
        self.assertEqual(BaseTest.USER_ID, log.user_id)
        self.assertEqual(FakeStorage.ACTION_TYPE_LEAVE, log.action_type)

        # check there's no users left in the group after leaving
        group_users = await self.group.get_users_in_group(group.group_id, None)  # noqa
        self.assertIsNotNone(group_users)
        self.assertEqual(0, group_users.user_count)
