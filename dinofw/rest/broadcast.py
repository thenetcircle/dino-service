from sqlalchemy.orm import Session

from dinofw.rest.base import BaseResource
from dinofw.rest.queries import BroadcastQuery


class MessageResource(BaseResource):
    async def send_message_to_group(
        self, query: BroadcastQuery, db: Session
    ) -> None:
        pass
