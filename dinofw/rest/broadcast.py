from sqlalchemy.orm import Session

from dinofw.rest.base import BaseResource


class BroadcastResource(BaseResource):
    async def send_message_to_group(
        self, group_id: str, event: dict, db: Session
    ) -> None:
        _, users_and_join_time, _ = self.env.db.get_users_in_group(group_id, db, include_group=False)
        user_ids = list(users_and_join_time.keys())

        self.env.client_publisher.send(user_ids, event)
