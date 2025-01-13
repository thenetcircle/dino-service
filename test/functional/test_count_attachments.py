import json

from dinofw.utils.config import MessageTypes, PayloadStatus
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestCountAttachments(BaseServerRestApi):
    async def test_count_increases(self):
        group_message = await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        group_id = group_message["group_id"]
        user_id = group_message["user_id"]
        await self.assert_attachment_count(group_id, user_id, 0)

        group_message = await self.send_1v1_message(
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
        await self.update_attachment(message_id, created_at, payload=json.dumps({
            "content": "some payload",
            "status": PayloadStatus.RESIZED
        }))

        await self.assert_attachment_count(group_id, user_id, 1)

    async def test_count_increases_in_cache(self):
        group_message = await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        group_id = group_message["group_id"]
        user_id = group_message["user_id"]

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertIsNone(the_count)

        group_message = await self.send_1v1_message(
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
        await self.update_attachment(message_id, created_at, payload=json.dumps({
            "content": "some payload",
            "status": PayloadStatus.RESIZED
        }))

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertEqual(1, the_count)

    async def test_count_removed_in_cache_when_removing_attachment(self):
        group_id, user_id = await self.create_attachment()

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertEqual(1, the_count)

        await self.delete_attachment(group_id, BaseTest.FILE_ID)

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertIsNone(the_count)

    async def test_count_removed_in_cache_when_updating_delete_before(self):
        group_message = await self.send_1v1_message()
        group_id, user_id = await self.create_attachment()

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertEqual(1, the_count)

        # should reset the count in the cache
        await self.update_delete_before(
            group_id, delete_before=group_message["created_at"], user_id=BaseTest.USER_ID
        )

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
        self.assertIsNone(the_count)

    async def test_count_multiple_attachments_delete_one_user(self):
        group_message = await self.send_1v1_message()
        await self.create_attachment()
        await self.create_attachment()
        group_id, user_id = await self.create_attachment()

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, BaseTest.USER_ID)
        self.assertEqual(3, the_count)
        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, BaseTest.OTHER_USER_ID)
        self.assertEqual(3, the_count)

        # should reset the count in the cache
        await self.update_delete_before(
            group_id, delete_before=group_message["created_at"], user_id=BaseTest.OTHER_USER_ID
        )

        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, BaseTest.USER_ID)
        self.assertEqual(3, the_count)
        the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, BaseTest.OTHER_USER_ID)
        self.assertIsNone(the_count)
