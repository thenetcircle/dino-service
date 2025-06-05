import asyncio
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestNicknameChange(BaseServerRestApi):
    async def test_nickname_change_does_not_undelete_for_others(self):
        await self.assert_groups_for_user(0, user_id=BaseTest.USER_ID)

        group_id = await self.create_and_join_group(
            user_id=BaseTest.USER_ID,
            users=[
                BaseTest.OTHER_USER_ID,
                BaseTest.THIRD_USER_ID
            ]
        )

        msgs = await self.send_message_to_group_from(group_id, user_id=BaseTest.USER_ID)
        creation_time = msgs[0]["created_at"]


        await self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)
        await self.assert_groups_for_user(1, user_id=BaseTest.THIRD_USER_ID)

        await self.update_delete_before(
            group_id=group_id,
            delete_before=creation_time,
            user_id=BaseTest.THIRD_USER_ID
        )
        await self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)

        await asyncio.sleep(0.02)  # wait so the deletion times and now are different

        # change nickname for other user, all default values
        await self.create_action_log_in_all_groups_for_user(
            user_id=BaseTest.THIRD_USER_ID
        )

        await asyncio.sleep(0.02)  # wait for the action log to be processed

        # should still be deleted for the third user
        await self.assert_groups_for_user(0, user_id=BaseTest.THIRD_USER_ID)
        await self.assert_groups_for_user(1, user_id=BaseTest.OTHER_USER_ID)
