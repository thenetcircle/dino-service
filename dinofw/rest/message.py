import logging
from typing import List

from sqlalchemy.orm import Session

from dinofw.db.cassandra.schemas import MessageBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import AdminQuery
from dinofw.rest.models import EditMessageQuery
from dinofw.rest.models import Message
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import SendMessageQuery

logger = logging.getLogger(__name__)


class MessageResource(BaseResource):
    def __init__(self, env):
        self.env = env

    async def save_new_message(self, group_id: str, user_id: int, query: SendMessageQuery, db: Session) -> Message:
        message = self.env.storage.store_message(group_id, user_id, query)

        self.env.db.update_group_new_message(message, db)
        self.env.db.update_last_read_in_group_for_user(user_id, group_id, message.created_at, db)

        return MessageResource.message_base_to_message(message)

    async def messages_in_group(self, group_id: str, query: MessageQuery) -> List[Message]:
        raw_messages = self.env.storage.get_messages_in_group(group_id, query)
        messages = list()

        for message_base in raw_messages:
            message = MessageResource.message_base_to_message(message_base)
            messages.append(message)

        return messages

    async def messages_for_user(
        self, group_id: str, user_id: int, query: MessageQuery
    ) -> List[Message]:
        raw_messages = self.env.storage.get_messages_in_group_for_user(group_id, user_id, query)
        messages = list()

        for message_base in raw_messages:
            message = MessageResource.message_base_to_message(message_base)
            messages.append(message)

        return messages

    async def edit_message(self, group_id: str, user_id: int, message_id: str, query: EditMessageQuery) -> None:
        self.env.storage.edit_message(group_id, user_id, message_id, query)

    async def delete_message(self, group_id: str, user_id: int, message_id: str, query: AdminQuery) -> None:
        self.env.storage.delete_message(group_id, user_id, message_id, query)

    async def message_details(self, group_id: str, user_id: int, message_id: str) -> Message:
        pass

    async def update_messages_for_user_in_group(self, group_id: str, user_id: int, query: MessageQuery) -> None:
        self.env.storage.update_messages_in_group_for_user(group_id, user_id, query)

    async def delete_messages_for_user_in_group(self, group_id: str, user_id: int, query: MessageQuery) -> None:
        self.env.storage.update_messages_in_group_for_user(group_id, user_id, query)

    async def update_messages(self, group_id: str, query: MessageQuery):
        self.env.storage.update_messages_in_group(group_id, query)

    async def delete_messages(self, group_id: str, query: MessageQuery):
        self.env.storage.delete_messages_in_group(group_id, query)

    @staticmethod
    def message_base_to_message(message: MessageBase) -> Message:
        message_dict = message.dict()

        message_dict["removed_at"] = MessageQuery.to_ts(message_dict["removed_at"], allow_none=True)
        message_dict["updated_at"] = MessageQuery.to_ts(message_dict["updated_at"], allow_none=True)
        message_dict["created_at"] = MessageQuery.to_ts(message_dict["created_at"], allow_none=True)

        return Message(**message_dict)
