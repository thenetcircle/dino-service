import logging
from typing import List

from sqlalchemy.orm import Session

from dinofw.rest.base import BaseResource
from dinofw.rest.models import AttachmentQuery
from dinofw.rest.models import CreateAttachmentQuery
from dinofw.rest.models import Message
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import SendMessageQuery
from dinofw.utils import utcnow_ts
from dinofw.utils.exceptions import NoSuchUserException
from dinofw.utils.exceptions import QueryValidationError

logger = logging.getLogger(__name__)


class MessageResource(BaseResource):
    async def send_message_to_group(
        self, group_id: str, user_id: int, query: SendMessageQuery, db: Session
    ) -> Message:
        message = self.env.storage.store_message(group_id, user_id, query)
        self._user_sends_a_message(group_id, user_id, message, db)

        return MessageResource.message_base_to_message(message)

    async def send_message_to_user(
        self, user_id: int, query: SendMessageQuery, db: Session
    ) -> Message:
        if query.receiver_id < 1:
            raise NoSuchUserException(query.receiver_id)

        group_id = await self._get_or_create_group_for_1v1(
            user_id, query.receiver_id, db
        )
        return await self.send_message_to_group(group_id, user_id, query, db)

    async def messages_in_group(
        self, group_id: str, query: MessageQuery
    ) -> List[Message]:
        raw_messages = self.env.storage.get_messages_in_group(group_id, query)
        messages = list()

        for message_base in raw_messages:
            message = MessageResource.message_base_to_message(message_base)
            messages.append(message)

        return messages

    async def messages_for_user(
        self, group_id: str, user_id: int, query: MessageQuery, db: Session
    ) -> List[Message]:
        user_stats = self.env.db.get_user_stats_in_group(group_id, user_id, db)

        if user_stats.hide:
            return list()

        raw_messages = self.env.storage.get_messages_in_group_for_user(
            group_id, user_stats, query
        )
        messages = list()

        for message_base in raw_messages:
            message = MessageResource.message_base_to_message(message_base)
            messages.append(message)

        return messages

    async def get_attachment_info(self, group_id: str, query: AttachmentQuery, db: Session) -> Message:
        group = self.env.db.get_group_from_id(group_id, db)

        message_base = self.env.storage.get_attachment_from_file_id(
            group_id,
            group.created_at,
            query
        )

        return MessageResource.message_base_to_message(message_base)

    async def create_attachment(
        self, user_id: int, message_id: str, query: CreateAttachmentQuery, db: Session
    ) -> Message:
        if query.group_id is None and query.receiver_id is None:
            raise QueryValidationError("both group_id and receiver_id is empty")
        elif query.group_id is not None and query.receiver_id is not None:
            raise QueryValidationError("can't use both group_id AND receiver_id, choose one")

        group_id = query.group_id

        if group_id is None:
            group_id = await self._get_or_create_group_for_1v1(
                user_id, query.receiver_id, db
            )

        attachment = self.env.storage.store_attachment(
            group_id, user_id, message_id, query
        )
        self._user_sends_an_attachment(group_id, attachment, db)

        return MessageResource.message_base_to_message(attachment)

    def delete_message(
        self, group_id: str, user_id: int, message_id: str, db: Session
    ) -> None:
        group = self.env.db.get_group_from_id(group_id, db)

        self.env.storage.delete_message(
            group_id, user_id, message_id, group.created_at
        )

        # TODO: how to tell apps a message was deleted? <-- update: create action log on deletions
        # TODO: self.env.publisher.delete_message(group_id, message_id)

    def delete_attachment(self, group_id: str, query: AttachmentQuery, db: Session) -> None:
        group = self.env.db.get_group_from_id(group_id, db)

        attachment = self.env.storage.delete_attachment(
            group_id, group.created_at, query
        )

        now = utcnow_ts()
        user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db).keys()

        self.create_action_log(query.action_log, db, group_id=group_id)
        self.env.server_publisher.delete_attachments(group_id, [attachment], user_ids, now)

    async def update_messages(self, group_id: str, query: MessageQuery):
        self.env.storage.update_messages_in_group(group_id, query)

    async def delete_messages(self, group_id: str, query: MessageQuery):
        self.env.storage.delete_messages_in_group(group_id, query)
