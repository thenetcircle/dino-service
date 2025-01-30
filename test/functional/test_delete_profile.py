from dinofw.utils.config import ErrorCodes
from test.functional.base_functional import BaseServerRestApi


class TestDeleteProfile(BaseServerRestApi):
    async def test_delete_profile(self):
        await self.send_1v1_message(receiver_id=1111)
        await self.send_1v1_message(receiver_id=2222)
        await self.send_1v1_message(receiver_id=3333)
        msg = await self.send_1v1_message(receiver_id=4444)
        group_id = msg["group_id"]

        await self.assert_groups_for_user(4)
        await self.delete_all_groups(create_action_log=False)
        await self.assert_groups_for_user(0)

        response = await self.histories_for(group_id, assert_response=False)
        self.assert_error(response, error_code=ErrorCodes.USER_NOT_IN_GROUP)

    async def test_delete_profile_with_action_log(self):
        await self.send_1v1_message(receiver_id=1111)
        await self.send_1v1_message(receiver_id=2222)
        await self.send_1v1_message(receiver_id=3333)
        msg = await self.send_1v1_message(receiver_id=4444)
        group_id = msg["group_id"]

        await self.assert_groups_for_user(4)
        await self.delete_all_groups(create_action_log=True)
        await self.assert_groups_for_user(0)

        response = await self.histories_for(group_id, assert_response=False)
        self.assert_error(response, error_code=ErrorCodes.USER_NOT_IN_GROUP)

    async def test_delete_profile_keeps_copy_in_deleted_table(self):
        await self.send_1v1_message(receiver_id=1111)
        await self.send_1v1_message(receiver_id=2222)
        await self.send_1v1_message(receiver_id=3333)
        await self.send_1v1_message(receiver_id=4444)

        await self.assert_deleted_groups_for_user(0)
        await self.assert_groups_for_user(4)

        await self.delete_all_groups(create_action_log=False)

        await self.assert_deleted_groups_for_user(4)
        await self.assert_groups_for_user(0)
