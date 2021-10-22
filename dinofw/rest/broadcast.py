from dinofw.rest.base import BaseResource
from dinofw.rest.queries import NotificationQuery


class BroadcastResource(BaseResource):
    async def broadcast_event(
        self, query: NotificationQuery
    ) -> None:
        # TODO: timeit
        for event in query.events:
            self.env.client_publisher.send_to_one(event.user_id, event.event)
