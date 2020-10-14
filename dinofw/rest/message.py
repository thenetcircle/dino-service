import logging
from typing import List

from sqlalchemy.orm import Session

from dinofw.rest.base import BaseResource
from dinofw.rest.models import AdminQuery
from dinofw.rest.models import CreateAttachmentQuery
from dinofw.rest.models import Message
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import SendMessageQuery
from dinofw.utils.exceptions import NoSuchGroupException, NoSuchUserException

logger = logging.getLogger(__name__)


class MessageResource(BaseResource):
    async def send_message_to_group(
        self, group_id: str, user_id: int, query: SendMessageQuery, db: Session
    ) -> Message:
        message = self.env.storage.store_message(group_id, user_id, query)
        self._user_sends_a_message(group_id, user_id, message, db)

        return MessageResource.message_base_to_message(message)

    async def _get_or_create_group_for_1v1(
        self, user_id: int, receiver_id: int, db: Session
    ) -> str:
        try:
            return self.env.db.get_group_id_for_1to1(user_id, receiver_id, db)
        except NoSuchGroupException:
            group = self.env.db.create_group_for_1to1(user_id, receiver_id, db)
            return group.group_id

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

    async def get_attachment_info(self, group_id: str, file_id: str, db: Session) -> Message:
        group = self.env.db.get_group_from_id(group_id, db)

        message_base = self.env.storage.get_attachment_from_file_id(
            group_id,
            group.created_at,
            file_id
        )

        return MessageResource.message_base_to_message(message_base)

    async def create_attachment(
        self, user_id: int, message_id: str, query: CreateAttachmentQuery, db: Session
    ) -> Message:
        group_id = await self._get_or_create_group_for_1v1(
            user_id, query.receiver_id, db
        )
        attachment = self.env.storage.store_attachment(
            group_id, user_id, message_id, query
        )
        self._user_sends_an_attachment(group_id, attachment, db)

        return MessageResource.message_base_to_message(attachment)

    async def delete_message(
        self, group_id: str, user_id: int, message_id: str, query: AdminQuery
    ) -> None:
        self.env.storage.delete_message(group_id, user_id, message_id, query)

    async def update_messages(self, group_id: str, query: MessageQuery):
        self.env.storage.update_messages_in_group(group_id, query)

    async def delete_messages(self, group_id: str, query: MessageQuery):
        self.env.storage.delete_messages_in_group(group_id, query)
