from dinofw.utils.config import GroupTypes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestOfflineUsers(BaseServerRestApi):
    # TODO: change to use the api to simulate the community removing offline users

    @BaseServerRestApi.init_db_session
    async def _test_offline_users_removed_from_rooms(self):
        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            group_type=GroupTypes.PUBLIC_ROOM
        )
        await self.user_joins_group(group_id, user_id=BaseTest.OTHER_USER_ID)

        await self.assert_user_in_group(group_id, user_id=BaseTest.USER_ID)
        await self.assert_user_in_group(group_id, user_id=BaseTest.OTHER_USER_ID)

        await self.env.db.remove_user_stats_for_offline_users(
            user_ids=[BaseTest.USER_ID, BaseTest.OTHER_USER_ID],
            db=self.env.db_session
        )

        # removed when going offline
        await self.assert_user_not_in_group(group_id, user_id=BaseTest.USER_ID)
        await self.assert_user_not_in_group(group_id, user_id=BaseTest.OTHER_USER_ID)

    @BaseServerRestApi.init_db_session
    async def _test_offline_users_not_affecting_private_groups(self):
        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            group_type=GroupTypes.PRIVATE_GROUP
        )
        await self.user_joins_group(group_id, user_id=BaseTest.OTHER_USER_ID)

        await self.assert_user_in_group(group_id, user_id=BaseTest.USER_ID)
        await self.assert_user_in_group(group_id, user_id=BaseTest.OTHER_USER_ID)

        await self.env.db.remove_user_stats_for_offline_users(
            user_ids=[BaseTest.USER_ID, BaseTest.OTHER_USER_ID],
            db=self.env.db_session
        )

        # still in the group after going offline
        await self.assert_user_in_group(group_id, user_id=BaseTest.USER_ID)
        await self.assert_user_in_group(group_id, user_id=BaseTest.OTHER_USER_ID)

    async def assert_user_in_group(self, group_id: str, user_id: int):
        await self._assert_user_in_group(group_id, user_id, should_exist=True)

    async def assert_user_not_in_group(self, group_id: str, user_id: int):
        await self._assert_user_in_group(group_id, user_id, should_exist=False)

    async def _assert_user_in_group(self, group_id: str, user_id: int, should_exist: bool):
        group = await self.get_group_info(group_id, count_messages=False)

        found = False
        for user in group["users"]:
            if user["user_id"] == user_id:
                found = True
                break

        self.assertEqual(found, should_exist)
