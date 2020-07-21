import logging
from datetime import datetime as dt
from typing import List

import pytz
from sqlalchemy.orm import Session

from dinofw.rest.server.base import BaseResource
from dinofw.rest.server.models import AdminQuery, GroupQuery
from dinofw.rest.server.models import EditMessageQuery
from dinofw.rest.server.models import Message
from dinofw.rest.server.models import MessageQuery
from dinofw.rest.server.models import SendMessageQuery

logger = logging.getLogger(__name__)


class MessageResource(BaseResource):
    async def save_new_message(
        self, group_id: str, user_id: int, query: SendMessageQuery, db: Session
    ) -> Message:
        message = self.env.storage.store_message(group_id, user_id, query)

        # cassandra DT is different from python DT
        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)

        self.env.db.update_group_new_message(message, now, db)
        self.env.db.update_last_read_and_sent_in_group_for_user(
            user_id, group_id, now, db
        )

        sub_query = GroupQuery(per_page=5_000)
        user_ids = self.env.db.get_user_ids_and_join_times_in_group(
            group_id, sub_query, db
        )
        self.env.publisher.message(group_id, user_id, message, user_ids)

        return MessageResource.message_base_to_message(message)

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
        self, group_id: str, user_id: int, query: MessageQuery
    ) -> List[Message]:
        raw_messages = self.env.storage.get_messages_in_group_for_user(
            group_id, user_id, query
        )
        messages = list()

        for message_base in raw_messages:
            message = MessageResource.message_base_to_message(message_base)
            messages.append(message)

        return messages

    async def edit_message(
        self, group_id: str, user_id: int, message_id: str, query: EditMessageQuery
    ) -> Message:
        message_base = self.env.storage.edit_message(
            group_id, user_id, message_id, query
        )

        return MessageResource.message_base_to_message(message_base)

    async def delete_message(
        self, group_id: str, user_id: int, message_id: str, query: AdminQuery
    ) -> None:
        self.env.storage.delete_message(group_id, user_id, message_id, query)

    async def message_details(
        self, group_id: str, user_id: int, message_id: str
    ) -> Message:
        message_base = self.env.storage.get_message(group_id, user_id, message_id)

        return MessageResource.message_base_to_message(message_base)

    async def update_messages_for_user_in_group(
        self, group_id: str, user_id: int, query: MessageQuery
    ) -> None:
        self.env.storage.update_messages_in_group_for_user(group_id, user_id, query)

    async def delete_messages_for_user_in_group(
        self, group_id: str, user_id: int, query: MessageQuery
    ) -> None:
        self.env.storage.update_messages_in_group_for_user(group_id, user_id, query)

    async def update_messages(self, group_id: str, query: MessageQuery):
        self.env.storage.update_messages_in_group(group_id, query)

    async def delete_messages(self, group_id: str, query: MessageQuery):
        self.env.storage.delete_messages_in_group(group_id, query)
