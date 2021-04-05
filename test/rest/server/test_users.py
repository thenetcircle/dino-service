from dinofw.rest.groups import GroupResource
from dinofw.rest.models import Group
from dinofw.rest.models import UserGroup
from dinofw.rest.queries import CreateGroupQuery
from dinofw.rest.queries import GroupQuery
from dinofw.rest.queries import UserStatsQuery
from dinofw.rest.users import UserResource
from test.base import BaseTest
from test.base import async_test


class TestUserResource(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.user = UserResource(self.fake_env)
        self.group = GroupResource(self.fake_env)

    @async_test
    async def test_get_groups_for_user(self):
        group_query = GroupQuery(per_page=10)
        create_query = CreateGroupQuery(
            group_name="some group name", group_type=0, users=[BaseTest.USER_ID],
        )

        # should be in zero groups in the beginning
        groups = await self.user.get_groups_for_user(
            BaseTest.USER_ID, group_query, None  # noqa
        )
        self.assertEqual(0, len(groups))

        # create and join a new group
        group1 = await self.group.create_new_group(
            BaseTest.USER_ID, create_query, None  # noqa
        )

        # get the stats and make sure we're only in one group
        groups = await self.user.get_groups_for_user(
            BaseTest.USER_ID, group_query, None  # noqa
        )
        self.assertEqual(1, len(groups))
        self.assertEqual(group1.group_id, groups[0].group.group_id)

        # create and join second new group
        group2 = await self.group.create_new_group(
            BaseTest.USER_ID, create_query, None  # noqa
        )

        # get the stats again, should be in two groups now
        groups = await self.user.get_groups_for_user(
            BaseTest.USER_ID, group_query, None  # noqa
        )
        self.assertEqual(2, len(groups))
        self.assertEqual(type(groups[0]), UserGroup)
        self.assertEqual(type(groups[0].group), Group)
        self.assertEqual(group2.group_id, groups[1].group.group_id)

    @async_test
    async def test_get_user_stats(self):
        create_query = CreateGroupQuery(
            group_name="some group name", group_type=0, users=[BaseTest.USER_ID],
        )

        # make sure we're not in a group in the beginning
        stats = await self.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(only_unread=False), None)  # noqa
        self.assertEqual(0, stats.group_amount)

        # create and join a new group
        await self.group.create_new_group(BaseTest.USER_ID, create_query, None)  # noqa

        # should be in the group we just created
        stats = await self.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(only_unread=False), None)  # noqa
        self.assertEqual(1, stats.group_amount)

        # create and join second new group
        await self.group.create_new_group(BaseTest.USER_ID, create_query, None)  # noqa

        # we should now be in two groups
        stats = await self.user.get_user_stats(BaseTest.USER_ID, UserStatsQuery(only_unread=False), None)  # noqa
        self.assertEqual(2, stats.group_amount)

        # check another user, should be in zero groups
        stats = await self.user.get_user_stats(BaseTest.OTHER_USER_ID, UserStatsQuery(only_unread=False), None)  # noqa
        self.assertEqual(0, stats.group_amount)
