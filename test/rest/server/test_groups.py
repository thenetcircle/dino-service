from dinofw.rest.server.groups import GroupResource
from dinofw.rest.server.message import MessageResource
from dinofw.rest.server.models import CreateGroupQuery, GroupUsers, Group, MessageQuery, SendMessageQuery
from test.base import async_test, BaseTest
from test.mocks import FakeEnv


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

        # create new group
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
