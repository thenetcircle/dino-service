from dinofw.utils.config import MessageTypes
from test.functional.base_functional import BaseServerRestApi


class TestSendAttachments(BaseServerRestApi):
    async def assert_is_in_attachment_table(self, message_type: int):
        group_message = await self.send_1v1_message(message_type=message_type)
        await self.update_attachment(group_message["message_id"], group_message["created_at"])

        atts = await self.attachments_for(group_message["group_id"])

        self.assertEqual(message_type, group_message["message_type"])
        self.assertEqual(message_type, atts[0]["message_type"])

    async def test_send_image_is_in_attachment_list(self):
        await self.assert_is_in_attachment_table(MessageTypes.IMAGE)

    async def test_send_video_is_in_attachment_list(self):
        await self.assert_is_in_attachment_table(MessageTypes.VIDEO)

    async def test_send_audio_is_not_in_attachment_list(self):
        group_message = await self.send_1v1_message(message_type=MessageTypes.AUDIO)
        await self.update_attachment(group_message["message_id"], group_message["created_at"])

        atts = await self.attachments_for(group_message["group_id"])

        self.assertEqual(MessageTypes.AUDIO, group_message["message_type"])
        self.assertEqual(0, len(atts))

        msgs = await self.histories_for(group_message["group_id"])
        self.assertEqual(1, len(msgs))
        self.assertEqual(MessageTypes.AUDIO, msgs["messages"][0]["message_type"])

    async def test_delete_attachments_audio_ignored(self):
        group_message = await self.send_1v1_message(message_type=MessageTypes.AUDIO)
        await self.update_attachment(group_message["message_id"], group_message["created_at"])

        atts = await self.attachments_for(group_message["group_id"])
        self.assertEqual(0, len(atts))

        msgs = await self.histories_for(group_message["group_id"])
        self.assertEqual(1, len(msgs))
        self.assertEqual(MessageTypes.AUDIO, msgs["messages"][0]["message_type"])

        await self.delete_attachments_in_all_groups(send_action_log_query=False)

        # still no attachments
        atts = await self.attachments_for(group_message["group_id"])
        self.assertEqual(0, len(atts))

        # audio remains in message history
        msgs = await self.histories_for(group_message["group_id"])
        self.assertEqual(1, len(msgs))
        self.assertEqual(MessageTypes.AUDIO, msgs["messages"][0]["message_type"])

    async def test_delete_video(self):
        group_message = await self.send_1v1_message(message_type=MessageTypes.VIDEO)
        await self.update_attachment(group_message["message_id"], group_message["created_at"])

        atts = await self.attachments_for(group_message["group_id"])
        self.assertEqual(1, len(atts))

        msgs = await self.histories_for(group_message["group_id"])
        self.assertEqual(1, len(msgs))
        self.assertEqual(MessageTypes.VIDEO, msgs["messages"][0]["message_type"])

        await self.delete_attachments_in_all_groups(send_action_log_query=False)

        # video deleted from attachments
        atts = await self.attachments_for(group_message["group_id"])
        self.assertEqual(0, len(atts))

        # video removed in history
        msgs = await self.histories_for(group_message["group_id"])
        self.assertEqual(0, len(msgs["messages"]))

    async def test_delete_image(self):
        group_message = await self.send_1v1_message(message_type=MessageTypes.IMAGE)
        await self.update_attachment(group_message["message_id"], group_message["created_at"])

        atts = await self.attachments_for(group_message["group_id"])
        self.assertEqual(1, len(atts))

        msgs = await self.histories_for(group_message["group_id"])
        self.assertEqual(1, len(msgs))
        self.assertEqual(MessageTypes.IMAGE, msgs["messages"][0]["message_type"])

        await self.delete_attachments_in_all_groups(send_action_log_query=False)

        # video deleted from attachments
        atts = await self.attachments_for(group_message["group_id"])
        self.assertEqual(0, len(atts))

        # video removed in history
        msgs = await self.histories_for(group_message["group_id"])
        self.assertEqual(0, len(msgs["messages"]))
