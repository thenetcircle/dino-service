import logging
from typing import List

from dinofw.rest.base import BaseResource
from dinofw.rest.models import SendMessageQuery, HistoryQuery, Message, EditMessageQuery, AdminQuery

logger = logging.getLogger(__name__)


class MessageResource(BaseResource):
    async def send(self, group_id: str, user_id: int, query: SendMessageQuery) -> None:
        pass

    async def edit(self, group_id: str, user_id: int, query: EditMessageQuery) -> None:
        pass

    async def delete(self, group_id: str, user_id: int, query: AdminQuery) -> None:
        pass

    async def messages(self, group_id: str, query: HistoryQuery) -> List[Message]:
        return [self._message(group_id)]

    async def messages_for_user(self, group_id: str, user_id: int, query: HistoryQuery) -> List[Message]:
        return [self._message(group_id, user_id)]
