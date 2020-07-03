import logging
from typing import List

from dinofw.rest.base import BaseResource
from dinofw.rest.models import (
    SendMessageQuery,
    HistoryQuery,
    Message,
    EditMessageQuery,
    AdminQuery,
    MessageQuery,
)

logger = logging.getLogger(__name__)


class MessageResource(BaseResource):
    def __init__(self, env):
        self.env = env

    async def send(self, group_id: str, user_id: int, query: SendMessageQuery) -> None:
        pass

    async def edit(self, group_id: str, user_id: int, query: EditMessageQuery) -> None:
        pass

    async def delete(self, group_id: str, user_id: int, query: AdminQuery) -> None:
        pass

    async def details(self, group_id: str, user_id: int, message_id: str) -> Message:
        pass

    async def messages(self, group_id: str, query: HistoryQuery) -> List[Message]:
        return [self._message(group_id)]

    async def messages_for_user(
        self, group_id: str, user_id: int, query: HistoryQuery
    ) -> List[Message]:
        return [self._message(group_id, user_id)]

    async def update_messages_for_user(
        self, group_id: str, user_id: int, query: MessageQuery
    ) -> List[Message]:
        pass

    async def delete_messages_for_user_in_group(
        self, group_id: str, user_id: int, query: AdminQuery
    ):
        pass

    async def update_messages(self, group_id: str, query: HistoryQuery):
        pass

    async def delete_messages(self, group_id: str, query: HistoryQuery):
        pass
