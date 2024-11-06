import datetime
from typing import Tuple
from uuid import uuid4 as uuid

import arrow

from dinofw.db.storage.schemas import MessageBase
from dinofw.utils.config import MessageTypes
from dinofw.rest.queries import CreateAttachmentQuery, SendMessageQuery
from dinofw.rest.queries import DeleteAttachmentQuery, AttachmentQuery
from dinofw.utils.exceptions import NoSuchMessageException, NoSuchAttachmentException
from dinofw.utils import utcnow_dt, utcnow_ts
from test.functional.cassandra.base_handler import BaseCassandraHandlerTest


class BaseAttachmentTest(BaseCassandraHandlerTest):

    def insert_attachment(self) -> Tuple[MessageBase, MessageBase]:
        msg = self.handler.store_message(
            BaseAttachmentTest.GROUP_ID,
            BaseAttachmentTest.USER_ID,
            SendMessageQuery(
                message_payload=BaseAttachmentTest.MESSAGE_PAYLOAD,
                message_type=MessageTypes.IMAGE,
            ),
        )
        atts = self.handler.store_attachment(
            BaseAttachmentTest.GROUP_ID,
            BaseAttachmentTest.USER_ID,
            msg.message_id,
            CreateAttachmentQuery(
                file_id=BaseAttachmentTest.FILE_ID,
                message_payload=BaseAttachmentTest.MESSAGE_PAYLOAD,
                created_at=utcnow_ts(),
            ),
        )
        return msg, atts

    def test_storage_attachment(self) -> None:
        self.clear_attachments()
        msg, atts = self.insert_attachment()
        # need timezone convention
        atts_dt = atts.created_at.replace(tzinfo=datetime.timezone.utc)
        with self.assertRaises(NoSuchMessageException):
            self.handler.store_attachment(
                BaseAttachmentTest.GROUP_ID,
                BaseAttachmentTest.USER_ID,
                str(uuid()),
                CreateAttachmentQuery(
                    file_id=BaseAttachmentTest.FILE_ID,
                    message_payload=BaseAttachmentTest.MESSAGE_PAYLOAD,
                    created_at=utcnow_ts(),
                ),
            )

        user = BaseAttachmentTest._generate_user_group_stats()
        attachment = self.handler.get_attachments_in_group_for_user(
            atts.group_id, user, BaseAttachmentTest._generate_message_query()
        )
        self.assertEqual(1, len(attachment))

        attachment = self.handler.get_attachment_from_file_id(
            atts.group_id,
            atts.created_at.replace(tzinfo=datetime.timezone.utc),
            AttachmentQuery(file_id=atts.file_id),
        )
        self.assertEqual(atts.message_payload, attachment.message_payload)
        with self.assertRaises(NoSuchAttachmentException):
            self.handler.get_attachment_from_file_id(
                atts.group_id, atts_dt, AttachmentQuery(file_id=str(uuid()))
            )

        count = self.handler.count_attachments_in_group_since(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO
        )
        self.assertEqual(1, count)
        count = self.handler.count_attachments_in_group_since(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO, atts.user_id
        )
        self.assertEqual(1, count)
        count = self.handler.count_attachments_in_group_since(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO, atts.user_id + 1
        )
        self.assertEqual(0, count)

        self.handler.delete_attachment(
            atts.group_id,
            arrow.get(atts_dt).shift(minutes=-1).datetime,
            DeleteAttachmentQuery(file_id=atts.file_id),
        )
        self.assert_get_attachments_in_group_for_user_empty()

    def test_delete_attachment(self) -> None:
        self.clear_attachments()
        for _ in range(3):
            self.insert_attachment()
        count = self.handler.count_attachments_in_group_since(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO
        )
        self.assertEqual(3, count)

        group_to_atts = self.handler.delete_attachments_in_all_groups(
            [], BaseAttachmentTest.USER_ID, DeleteAttachmentQuery()
        )
        self.assertEqual(0, len(group_to_atts))
        count = self.handler.count_attachments_in_group_since(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO
        )
        self.assertEqual(3, count)

        group_to_atts = self.handler.delete_attachments_in_all_groups(
            [(BaseAttachmentTest.GROUP_ID, utcnow_dt())],
            BaseAttachmentTest.USER_ID,
            DeleteAttachmentQuery(),
        )
        self.assertEqual(0, len(group_to_atts))
        count = self.handler.count_attachments_in_group_since(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO
        )
        self.assertEqual(3, count)

        group_to_atts = self.handler.delete_attachments_in_all_groups(
            [(BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO)],
            BaseAttachmentTest.USER_ID,
            DeleteAttachmentQuery(),
        )
        self.assertEqual(1, len(group_to_atts))
        self.assertIn(BaseAttachmentTest.GROUP_ID, group_to_atts)
        self.assertEqual(3, len(group_to_atts[BaseAttachmentTest.GROUP_ID]))
        count = self.handler.count_attachments_in_group_since(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO
        )
        self.assertEqual(0, count)

        for _ in range(3):
            self.insert_attachment()
        count = self.handler.count_attachments_in_group_since(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO
        )
        self.assertEqual(3, count)

        self.handler.delete_attachments_in_group_before(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO
        )
        count = self.handler.count_attachments_in_group_since(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO
        )
        self.assertEqual(3, count)
        self.handler.delete_attachments_in_group_before(
            BaseAttachmentTest.GROUP_ID, BaseAttachmentTest.LONG_AGO
        )
        count = self.handler.count_attachments_in_group_since(
            BaseAttachmentTest.GROUP_ID, utcnow_dt()
        )
        self.assertEqual(0, count)
