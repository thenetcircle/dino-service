import json

from dinofw.utils.config import MessageTypes, ErrorCodes, PayloadStatus
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestDeleteAttachments(BaseServerRestApi):
    async def test_payload_status_updated(self):
        group_message = await self.send_1v1_message(
            message_type=MessageTypes.IMAGE,
            payload=json.dumps({
                "content": "some payload",
                "status": PayloadStatus.PENDING
            })
        )
        message_id = group_message["message_id"]
        created_at = group_message["created_at"]
        group_id = group_message["group_id"]

        # sets the file id we later delete by
        await self.update_attachment(message_id, created_at, payload=json.dumps({
            "content": "some payload",
            "status": PayloadStatus.RESIZED
        }))

        attachment = await self.attachment_for_file_id(group_id, BaseTest.FILE_ID)
        self.assertIsNotNone(attachment)

        histories = await self.histories_for(group_id)
        self.assertEqual(1, len(histories["messages"]))

        message = histories["messages"][0]
        payload = json.loads(message["message_payload"])
        self.assertEqual(payload["status"], PayloadStatus.RESIZED)

        await self.delete_attachment(group_id)

        # next time we check it shouldn't exist
        att = await self.attachment_for_file_id(group_id, BaseTest.FILE_ID, assert_response=False)
        self.assert_error(att, ErrorCodes.NO_SUCH_ATTACHMENT)

        histories = await self.histories_for(group_id)
        self.assertEqual(1, len(histories["messages"]))

        message = histories["messages"][0]
        payload = json.loads(message["message_payload"])
        self.assertEqual(payload["status"], PayloadStatus.DELETED)

    async def test_delete_one_attachment(self):
        group_message = await self.send_1v1_message(message_type=MessageTypes.IMAGE)
        message_id = group_message["message_id"]
        created_at = group_message["created_at"]
        group_id = group_message["group_id"]

        # sets the file id we later delete by
        await self.update_attachment(message_id, created_at)

        attachment = await self.attachment_for_file_id(group_id, BaseTest.FILE_ID)
        self.assertIsNotNone(attachment)

        await self.delete_attachment(group_id)

        # next time we check it shouldn't exist
        att = await self.attachment_for_file_id(group_id, BaseTest.FILE_ID, assert_response=False)
        self.assert_error(att, ErrorCodes.NO_SUCH_ATTACHMENT)

    async def test_delete_all_attachments_in_one_group_for_user(self):
        group_id = None

        for file_id in [str(i) for i in range(10)]:
            group_message = await self.send_1v1_message(message_type=MessageTypes.IMAGE)
            message_id = group_message["message_id"]
            group_id = group_message["group_id"]
            created_at = group_message["created_at"]

            await self.update_attachment(message_id, created_at, file_id=file_id)

        for file_id in [str(i) for i in range(10)]:
            attachment = await self.attachment_for_file_id(group_id, file_id)
            self.assertNotIn("detail", attachment)  # will have 'detail' if there was an error
            self.assertIsNotNone(attachment)

        await self.delete_attachments_in_group(group_id)

        # next time we check it shouldn't exist
        for file_id in [str(i) for i in range(10)]:
            att = await self.attachment_for_file_id(group_id, file_id, assert_response=False)
            self.assert_error(att, ErrorCodes.NO_SUCH_ATTACHMENT)

    async def test_delete_all_attachments_in_all_groups_for_user_with_action_logs(self):
        await self._delete_all_attachments_in_all_groups_for_user(
            create_action_logs=True
        )

    async def _delete_all_attachments_in_all_groups_for_user(self, create_action_logs: bool):
        group_id = None

        file_ids = {
            BaseTest.USER_ID: [str(i) for i in range(5)],
            BaseTest.OTHER_USER_ID: [str(i + 10) for i in range(5)],
        }

        for user_id in [BaseTest.USER_ID, BaseTest.OTHER_USER_ID]:
            for file_id in file_ids[user_id]:
                group_message = await self.send_1v1_message(message_type=MessageTypes.IMAGE, user_id=user_id)
                message_id = group_message["message_id"]
                group_id = group_message["group_id"]
                created_at = group_message["created_at"]

                await self.update_attachment(message_id, created_at, user_id=user_id, file_id=file_id)

            for file_id in file_ids[user_id]:
                attachment = await self.attachment_for_file_id(group_id, file_id)
                self.assertNotIn("detail", attachment)  # will have 'detail' if there was an error
                self.assertIsNotNone(attachment)

        self.assertEqual(0, len(self.env.client_publisher.sent_deletions))
        self.assertEqual(0, len(self.env.server_publisher.sent_deletions))

        await self.delete_attachments_in_all_groups(send_action_log_query=create_action_logs)

        # next time we check it shouldn't exist
        for should_exist, user_id in [(False, BaseTest.USER_ID), (True, BaseTest.OTHER_USER_ID)]:
            for file_id in file_ids[user_id]:
                if should_exist:
                    attachment = await self.attachment_for_file_id(group_id, file_id)
                    self.assertNotIn("detail", attachment)
                    self.assertIsNotNone(attachment)
                else:
                    att = await self.attachment_for_file_id(group_id, file_id, assert_response=False)
                    self.assert_error(att, ErrorCodes.NO_SUCH_ATTACHMENT)

        # only one deletion event even though we deleted five attachments
        # if create_action_logs:
        #     # one message, one deletion; client published don't distinguish between the two, both are messages
        #     self.assertEqual(2, len(self.env.client_publisher.sent_messages))

        self.assertEqual(1, len(self.env.server_publisher.sent_deletions))
