from dinofw.rest.server.groups import GroupResource
from dinofw.rest.server.models import CreateGroupQuery, GroupUsers, Group
from test.base import async_test, BaseTest
from test.mocks import FakeEnv


class TestGroupResource(BaseTest):
    def setUp(self) -> None:
        self.resource = GroupResource(FakeEnv())

    @async_test
    async def test_create_new_group(self):
        group_name = "new group name"
        query = CreateGroupQuery(
            group_name=group_name,
            group_type=0,
            users=[BaseTest.USER_ID],
        )

        self.assertEqual(0, len(self.resource.env.storage.action_log))

        group = await self.resource.create_new_group(
            user_id=BaseTest.USER_ID,
            query=query,
            db=None  # noqa
        )

        self.assertIsNotNone(group)
        self.assertEqual(type(group), Group)
        self.assertEqual(group_name, group.name)
        self.assertEqual(1, len(self.resource.env.storage.action_log))
        self.assertEqual(1, len(self.resource.env.storage.action_log[group.group_id]))

    @async_test
    async def test_get_users_in_group(self):
        query = CreateGroupQuery(
            group_name="a group name",
            group_type=0,
            users=[BaseTest.USER_ID],
        )
        group = await self.resource.create_new_group(
            user_id=BaseTest.USER_ID,
            query=query,
            db=None  # noqa
        )

        group_users = await self.resource.get_users_in_group(
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
        group = await self.resource.get_group(
            group_id=BaseTest.GROUP_ID,
            db=None  # noqa
        )

        self.assertIsNone(group)

        query = CreateGroupQuery(
            group_name="some group name",
            group_type=0,
            users=[BaseTest.USER_ID],
        )
        group = await self.resource.create_new_group(
            user_id=BaseTest.USER_ID,
            query=query,
            db=None  # noqa
        )

        self.assertIsNotNone(group)
        self.assertEqual(type(group), Group)

        group = await self.resource.get_group(
            group_id=group.group_id,
            db=None  # noqa
        )

        self.assertIsNotNone(group)
        self.assertEqual(type(group), Group)
