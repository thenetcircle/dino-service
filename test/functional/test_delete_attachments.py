import json

from dinofw.utils.config import MessageTypes, ErrorCodes, PayloadStatus
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestReceiverStats(BaseServerRestApi):
    def test_payload_status_updated(self):
        message = self.send_1v1_message(
            message_type=MessageTypes.IMAGE,
            payload=json.dumps({
                "content": "some payload",
                "status": PayloadStatus.PENDING
            })
        )
        message_id = message["message_id"]
        created_at = message["created_at"]
        group_id = message["group_id"]

        # sets the file id we later delete by
        self.update_attachment(message_id, created_at, payload=json.dumps({
            "content": "some payload",
            "status": PayloadStatus.RESIZED
        }))

        attachment = self.attachment_for_file_id(group_id, BaseTest.FILE_ID)
        self.assertIsNotNone(attachment)

        histories = self.histories_for(group_id)
        self.assertEqual(1, len(histories["messages"]))

        message = histories["messages"][0]
        payload = json.loads(message["message_payload"])
        self.assertEqual(payload["status"], PayloadStatus.RESIZED)

        self.delete_attachment(group_id)

        # next time we check it shouldn't exist
        att = self.attachment_for_file_id(group_id, BaseTest.FILE_ID, assert_response=False)
        self.assert_error(att, ErrorCodes.NO_SUCH_ATTACHMENT)

        histories = self.histories_for(group_id)
        self.assertEqual(1, len(histories["messages"]))

        message = histories["messages"][0]
        payload = json.loads(message["message_payload"])
        self.assertEqual(payload["status"], PayloadStatus.DELETED)

    def test_delete_one_attachment(self):
        message = self.send_1v1_message(message_type=MessageTypes.IMAGE)
        message_id = message["message_id"]
        created_at = message["created_at"]
        group_id = message["group_id"]

        # sets the file id we later delete by
        self.update_attachment(message_id, created_at)

        attachment = self.attachment_for_file_id(group_id, BaseTest.FILE_ID)
        self.assertIsNotNone(attachment)

        self.delete_attachment(group_id)

        # next time we check it shouldn't exist
        att = self.attachment_for_file_id(group_id, BaseTest.FILE_ID, assert_response=False)
        self.assert_error(att, ErrorCodes.NO_SUCH_ATTACHMENT)

    def test_delete_all_attachments_in_one_group_for_user(self):
        group_id = None

        for file_id in [str(i) for i in range(10)]:
            message = self.send_1v1_message(message_type=MessageTypes.IMAGE)
            message_id = message["message_id"]
            group_id = message["group_id"]
            created_at = message["created_at"]

            self.update_attachment(message_id, created_at, file_id=file_id)

        for file_id in [str(i) for i in range(10)]:
            attachment = self.attachment_for_file_id(group_id, file_id)
            self.assertNotIn("detail", attachment)  # will have 'detail' if there was an error
            self.assertIsNotNone(attachment)

        self.delete_attachments_in_group(group_id)

        # next time we check it shouldn't exist
        for file_id in [str(i) for i in range(10)]:
            att = self.attachment_for_file_id(group_id, file_id, assert_response=False)
            self.assert_error(att, ErrorCodes.NO_SUCH_ATTACHMENT)

    def test_delete_all_attachments_in_all_groups_for_user_with_action_logs(self):
        self._delete_all_attachments_in_all_groups_for_user(
            create_action_logs=True
        )

    def _delete_all_attachments_in_all_groups_for_user(self, create_action_logs: bool):
        group_id = None

        file_ids = {
            BaseTest.USER_ID: [str(i) for i in range(5)],
            BaseTest.OTHER_USER_ID: [str(i + 10) for i in range(5)],
        }

        for user_id in [BaseTest.USER_ID, BaseTest.OTHER_USER_ID]:
            for file_id in file_ids[user_id]:
                message = self.send_1v1_message(message_type=MessageTypes.IMAGE, user_id=user_id)
                message_id = message["message_id"]
                group_id = message["group_id"]
                created_at = message["created_at"]

                self.update_attachment(message_id, created_at, user_id=user_id, file_id=file_id)

            for file_id in file_ids[user_id]:
                attachment = self.attachment_for_file_id(group_id, file_id)
                self.assertNotIn("detail", attachment)  # will have 'detail' if there was an error
                self.assertIsNotNone(attachment)

        self.assertEqual(0, len(self.env.client_publisher.sent_deletions))
        self.assertEqual(0, len(self.env.server_publisher.sent_deletions))

        self.delete_attachments_in_all_groups(send_action_log_query=create_action_logs)

        # next time we check it shouldn't exist
        for should_exist, user_id in [(False, BaseTest.USER_ID), (True, BaseTest.OTHER_USER_ID)]:
            for file_id in file_ids[user_id]:
                if should_exist:
                    attachment = self.attachment_for_file_id(group_id, file_id)
                    self.assertNotIn("detail", attachment)
                    self.assertIsNotNone(attachment)
                else:
                    att = self.attachment_for_file_id(group_id, file_id, assert_response=False)
                    self.assert_error(att, ErrorCodes.NO_SUCH_ATTACHMENT)

        # only one deletion event even though we deleted five attachments
        if create_action_logs:
            # one message, one deletion; client published don't distinguish between the two, both are messages
            self.assertEqual(2, len(self.env.client_publisher.sent_messages))

        self.assertEqual(1, len(self.env.server_publisher.sent_deletions))
