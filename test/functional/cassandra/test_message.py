import datetime
from uuid import uuid4 as uuid

from dinofw.utils.config import MessageTypes
from dinofw.rest.queries import SendMessageQuery, AdminQuery, ActionLogQuery
from dinofw.rest.queries import EditMessageQuery
from dinofw.utils.exceptions import NoSuchMessageException
from dinofw.utils import utcnow_dt, utcnow_ts
from test.functional.cassandra.base_handler import BaseCassandraHandlerTest


class BaseMessageTest(BaseCassandraHandlerTest):

    def test_store_message(self) -> None:
        self.clear_messages()
        msg = self.handler.store_message(
            BaseMessageTest.GROUP_ID,
            BaseMessageTest.USER_ID,
            SendMessageQuery(
                message_payload=BaseMessageTest.MESSAGE_PAYLOAD,
                message_type=MessageTypes.MESSAGE,
            ),
        )
        message = self.handler.get_all_messages_in_group(msg.group_id)
        self.assertEqual(1, len(message))

        user = BaseMessageTest._generate_user_group_stats()
        user.last_sent = msg.created_at
        message = self.handler.get_messages_in_group_only_from_user(
            msg.group_id, user, BaseMessageTest._generate_message_query()
        )
        self.assertEqual(1, len(message))
        message = self.handler.get_messages_in_group_for_user(
            msg.group_id, user, BaseMessageTest._generate_message_query()
        )
        self.assertEqual(1, len(message))
        user.user_id += 1
        message = self.handler.get_messages_in_group_only_from_user(
            msg.group_id, user, BaseMessageTest._generate_message_query()
        )
        self.assertEqual(0, len(message))
        # user id irrelevant
        message = self.handler.get_messages_in_group_for_user(
            msg.group_id, user, BaseMessageTest._generate_message_query()
        )
        self.assertEqual(1, len(message))

        message = self.handler.get_messages_in_group(
            msg.group_id, BaseMessageTest._generate_message_query()
        )
        self.assertEqual(1, len(message))
        message = self.handler.get_messages_in_group(
            msg.group_id,
            BaseMessageTest._generate_message_query(
                until=BaseMessageTest.LONG_AGO.timestamp()
            ),
        )
        self.assertEqual(0, len(message))
        # since does not work
        message = self.handler.get_messages_in_group(
            msg.group_id, BaseMessageTest._generate_message_query(since=utcnow_ts())
        )
        self.assertEqual(1, len(message))

        self.handler.get_message_with_id(
            msg.group_id, msg.user_id, msg.message_id, msg.created_at.timestamp()
        )
        self.handler.get_message_with_id(
            msg.group_id, msg.user_id, msg.message_id, msg.created_at.timestamp() - 59
        )
        self.handler.get_message_with_id(
            msg.group_id, msg.user_id, msg.message_id, msg.created_at.timestamp() + 59
        )
        # invalid range
        with self.assertRaises(NoSuchMessageException):
            self.handler.get_message_with_id(
                msg.group_id,
                msg.user_id,
                msg.message_id,
                msg.created_at.timestamp() - 61,
            )
        with self.assertRaises(NoSuchMessageException):
            self.handler.get_message_with_id(
                msg.group_id,
                msg.user_id,
                msg.message_id,
                msg.created_at.timestamp() + 61,
            )
        # invalid id
        with self.assertRaises(NoSuchMessageException):
            self.handler.get_message_with_id(
                str(uuid()), msg.user_id, msg.message_id, msg.created_at.timestamp()
            )
        with self.assertRaises(NoSuchMessageException):
            self.handler.get_message_with_id(
                msg.group_id,
                msg.user_id + 1,
                msg.message_id,
                msg.created_at.timestamp(),
            )
        with self.assertRaises(NoSuchMessageException):
            self.handler.get_message_with_id(
                msg.group_id, msg.user_id, str(uuid()), msg.created_at.timestamp()
            )

        offset = self.handler.get_created_at_for_offset(msg.group_id, 0)
        self.assertEqual(msg.created_at, offset.replace(tzinfo=datetime.timezone.utc))

        count = self.handler.count_messages_in_group_since(
            msg.group_id, since=BaseMessageTest.LONG_AGO
        )
        self.assertEqual(1, count)
        count = self.handler.count_messages_in_group_since(
            msg.group_id, since=utcnow_dt()
        )
        self.assertEqual(0, count)
        # mock admin query
        count = self.handler.count_messages_in_group_since(
            msg.group_id,
            since=utcnow_dt(),
            query=AdminQuery(admin_id=BaseMessageTest.ADMIN_ID, include_deleted=True),
        )
        self.assertEqual(1, count)

        count = self.handler.count_messages_in_group_from_user_since(
            msg.group_id,
            msg.user_id,
            utcnow_dt(),
            BaseMessageTest.LONG_AGO,
        )
        self.assertEqual(1, count)
        count = self.handler.count_messages_in_group_from_user_since(
            msg.group_id,
            msg.user_id,
            utcnow_dt(),
            utcnow_dt(),
        )
        self.assertEqual(0, count)
        count = self.handler.count_messages_in_group_from_user_since(
            msg.group_id,
            msg.user_id,
            utcnow_dt(),
            utcnow_dt(),
            query=AdminQuery(admin_id=BaseMessageTest.ADMIN_ID, include_deleted=True),
        )
        self.assertEqual(1, count)

        # self.handler.delete_message(msg.group_id, msg.user_id, msg.message_id, msg.created_at)
        # self.assert_get_messages_in_group_empty()

    def test_create_action_log(self) -> None:
        self.clear_messages()
        msg = self.handler.create_action_log(
            BaseMessageTest.USER_ID,
            BaseMessageTest.GROUP_ID,
            ActionLogQuery(payload=BaseMessageTest.MESSAGE_PAYLOAD),
        )
        message = self.handler.get_all_messages_in_group(msg.group_id)
        self.assertEqual(1, len(message))

        new_payload = "edited action log"
        self.handler.edit_message(
            msg.group_id,
            msg.user_id,
            msg.message_id,
            EditMessageQuery(
                created_at=msg.created_at.timestamp(), message_payload=new_payload
            ),
        )
        # invalid range
        with self.assertRaises(NoSuchMessageException):
            self.handler.edit_message(
                msg.group_id,
                msg.user_id,
                msg.message_id,
                EditMessageQuery(
                    created_at=BaseMessageTest.LONG_AGO.timestamp(),
                    message_payload=new_payload,
                ),
            )
        # invalid id
        with self.assertRaises(NoSuchMessageException):
            self.handler.edit_message(
                str(uuid()),
                msg.user_id,
                msg.message_id,
                EditMessageQuery(
                    created_at=msg.created_at.timestamp(), message_payload=new_payload
                ),
            )
        with self.assertRaises(NoSuchMessageException):
            self.handler.edit_message(
                msg.group_id,
                msg.user_id + 1,
                msg.message_id,
                EditMessageQuery(
                    created_at=msg.created_at.timestamp(), message_payload=new_payload
                ),
            )
        with self.assertRaises(NoSuchMessageException):
            self.handler.edit_message(
                msg.group_id,
                msg.user_id,
                str(uuid()),
                EditMessageQuery(
                    created_at=msg.created_at.timestamp(), message_payload=new_payload
                ),
            )

        message = self.handler.get_message_with_id(
            msg.group_id, msg.user_id, msg.message_id, msg.created_at.timestamp()
        )
        self.assertEqual(new_payload, message.message_payload)

        self.handler.delete_message(
            msg.group_id, msg.user_id, msg.message_id, msg.created_at
        )
        self.assert_get_messages_in_group_empty()
