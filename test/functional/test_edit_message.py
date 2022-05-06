from dinofw.utils.config import ErrorCodes, MessageTypes, PayloadStatus
from test.base import BaseTest
import json
from test.functional.base_functional import BaseServerRestApi


class TestEditMessage(BaseServerRestApi):
    def test_edit_payload(self):
        old_payload = "some payload"
        new_payload = "updated payload"

        message = self.send_1v1_message(
            message_type=MessageTypes.IMAGE,
            payload=json.dumps({
                "content": old_payload,
                "status": PayloadStatus.PENDING
            })
        )

        self.edit_message(
            message["group_id"],
            message["message_id"],
            message["created_at"],
            new_payload=json.dumps({
                "content": new_payload,
                "status": PayloadStatus.PENDING
            })
        )

        info = self.get_message_info(
            user_id=BaseTest.USER_ID,
            message_id=message["message_id"],
            group_id=message["group_id"],
            created_at=message["created_at"],
            expected_response_code=200
        )

        self.assertEqual(new_payload, info["message_payload"])
