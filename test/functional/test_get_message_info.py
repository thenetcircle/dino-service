from dinofw.utils.config import ErrorCodes
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestGetMessageInfo(BaseServerRestApi):
    async def test_get_message_info_1v1(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message()

        info = await self.get_message_info(
            user_id=BaseTest.USER_ID,
            message_id=group_message["message_id"],
            group_id=group_message["group_id"],
            created_at=group_message["created_at"],
            expected_response_code=200
        )

        self.assertEqual(group_message["message_payload"], info["message_payload"])

    async def test_get_message_info_1v1_wrong_created_at(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message()

        response = await self.get_message_info(
            user_id=BaseTest.USER_ID,
            message_id=group_message["message_id"],
            group_id=group_message["group_id"],
            created_at=group_message["created_at"] - 3600,
            expected_response_code=400
        )

        self.assertEqual(int(response["detail"].split(":")[0]), ErrorCodes.NO_SUCH_MESSAGE)

    async def test_get_message_info_1v1_wrong_user_id(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message()

        response = await self.get_message_info(
            user_id=BaseTest.OTHER_USER_ID,
            message_id=group_message["message_id"],
            group_id=group_message["group_id"],
            created_at=group_message["created_at"],
            expected_response_code=400
        )

        self.assertEqual(int(response["detail"].split(":")[0]), ErrorCodes.NO_SUCH_MESSAGE)

    async def test_get_message_info_1v1_wrong_group_id(self):
        await self.assert_groups_for_user(0)
        group_message = await self.send_1v1_message()

        response = await self.get_message_info(
            user_id=BaseTest.USER_ID,
            message_id=group_message["message_id"],
            group_id="bad-group-id",
            created_at=group_message["created_at"],
            expected_response_code=400
        )

        self.assertEqual(int(response["detail"].split(":")[0]), ErrorCodes.NO_SUCH_MESSAGE)
