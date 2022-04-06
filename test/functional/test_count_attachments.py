import json

from dinofw.utils.config import MessageTypes, PayloadStatus
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestCountAttachments(BaseServerRestApi):
    def test_count_increases(self):
        group_message = self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        group_id = group_message["group_id"]
        user_id = group_message["user_id"]
        self.assert_attachment_count(group_id, user_id, 0)

        group_message = self.send_1v1_message(
            message_type=MessageTypes.IMAGE,
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID,
            payload=json.dumps({
                "content": "some payload",
                "status": PayloadStatus.PENDING
            })
        )

        message_id = group_message["message_id"]
        created_at = group_message["created_at"]
        self.update_attachment(message_id, created_at, payload=json.dumps({
            "content": "some payload",
            "status": PayloadStatus.RESIZED
        }))

        self.assert_attachment_count(group_id, user_id, 1)

    def test_count_increases_in_cache(self):
        group_message = self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        group_id = group_message["group_id"]
        user_id = group_message["user_id"]

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertIsNone(the_count)

        group_message = self.send_1v1_message(
            message_type=MessageTypes.IMAGE,
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID,
            payload=json.dumps({
                "content": "some payload",
                "status": PayloadStatus.PENDING
            })
        )

        message_id = group_message["message_id"]
        created_at = group_message["created_at"]
        self.update_attachment(message_id, created_at, payload=json.dumps({
            "content": "some payload",
            "status": PayloadStatus.RESIZED
        }))

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertEqual(1, the_count)

    def test_count_removed_in_cache_when_removing_attachment(self):
        group_id, user_id = self._create_attachment()

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertEqual(1, the_count)

        self.delete_attachment(group_id, BaseTest.FILE_ID)

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertIsNone(the_count)

    def test_count_removed_in_cache_when_updating_delete_before(self):
        group_message = self.send_1v1_message()
        group_id, user_id = self._create_attachment()

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertEqual(1, the_count)

        # should reset the count in the cache
        self.update_delete_before(
            group_id, delete_before=group_message["created_at"], user_id=BaseTest.USER_ID
        )

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertIsNone(the_count)

    def test_count_multiple_attachments_delete_one_user(self):
        group_message = self.send_1v1_message()
        self._create_attachment()
        self._create_attachment()
        group_id, user_id = self._create_attachment()

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, BaseTest.USER_ID)
        self.assertEqual(3, the_count)
        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, BaseTest.OTHER_USER_ID)
        self.assertEqual(3, the_count)

        # should reset the count in the cache
        self.update_delete_before(
            group_id, delete_before=group_message["created_at"], user_id=BaseTest.OTHER_USER_ID
        )

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, BaseTest.USER_ID)
        self.assertEqual(3, the_count)
        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, BaseTest.OTHER_USER_ID)
        self.assertIsNone(the_count)

    def _create_attachment(self):
        group_message = self.send_1v1_message(
            message_type=MessageTypes.IMAGE,
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID,
            payload=json.dumps({
                "content": "some payload",
                "status": PayloadStatus.PENDING
            })
        )

        message_id = group_message["message_id"]
        created_at = group_message["created_at"]
        group_id = group_message["group_id"]
        user_id = group_message["user_id"]

        self.update_attachment(
            message_id=message_id,
            created_at=created_at,
            file_id=BaseTest.FILE_ID,
            payload=json.dumps({
                "content": "some payload",
                "status": PayloadStatus.RESIZED
            })
        )

        return group_id, user_id

    def assert_attachment_count(self, group_id: str, user_id: int, expected_amount: int):
        raw_response = self.client.post(
            f"/v1/groups/{group_id}/user/{user_id}/count", json={
                "only_attachments": True
            },
        )
        self.assertEqual(raw_response.status_code, 200)

        group = raw_response.json()
        self.assertEqual(expected_amount, group["message_count"])
