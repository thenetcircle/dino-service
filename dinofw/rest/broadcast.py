from dinofw.rest.base import BaseResource
from dinofw.rest.queries import BroadcastQuery


class MessageResource(BaseResource):
    async def send_message_to_group(
        self, query: BroadcastQuery
    ) -> None:
        self.env.client_publisher.send(query.user_ids, query.context)
