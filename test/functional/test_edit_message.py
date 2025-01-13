from dinofw.utils.config import ErrorCodes, MessageTypes, PayloadStatus
from test.base import BaseTest
import json
from test.functional.base_functional import BaseServerRestApi


class TestEditMessage(BaseServerRestApi):
    async def test_edit_payload(self):
        old_payload = "some payload"
        new_payload = "updated payload"

        message = await self.send_1v1_message(
            message_type=MessageTypes.IMAGE,
            payload=json.dumps({
                "content": old_payload,
                "status": PayloadStatus.PENDING
            })
        )
        info = await self.get_message_info(
            user_id=BaseTest.USER_ID,
            message_id=message["message_id"],
            group_id=message["group_id"],
            created_at=message["created_at"],
            expected_response_code=200
        )
        self.assertEqual(old_payload, json.loads(info["message_payload"])["content"])

        await self.edit_message(
            user_id=message["user_id"],
            group_id=message["group_id"],
            message_id=message["message_id"],
            created_at=message["created_at"],
            new_payload=json.dumps({
                "content": new_payload,
                "status": PayloadStatus.PENDING
            })
        )

        info = await self.get_message_info(
            user_id=BaseTest.USER_ID,
            message_id=message["message_id"],
            group_id=message["group_id"],
            created_at=message["created_at"],
            expected_response_code=200
        )
        self.assertEqual(new_payload, json.loads(info["message_payload"])["content"])
