from dinofw.utils.config import GroupTypes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestDeleteGroup(BaseServerRestApi):
    async def test_delete_group_creates_deleted_user_copy(self):
        await self.assert_deleted_groups_for_user(0)
        await self.assert_groups_for_user(0)

        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ],
            group_type=GroupTypes.PUBLIC_ROOM
        )

        await self.assert_deleted_groups_for_user(0)
        await self.assert_groups_for_user(0)
        await self.assert_public_groups_for_user(1)
        self.assertEqual(0, len(self.env.storage.action_log))

        await self.update_group_deleted(group_id, deleted=True)

        # we don't create deletion logs for public groups
        await self.assert_deleted_groups_for_user(1)
        await self.assert_groups_for_user(0)
        await self.assert_public_groups_for_user(0)

        # should have an action log for deleting a public group
        self.assertEqual(1, len(self.env.storage.action_log))
