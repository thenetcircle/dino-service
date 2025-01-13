import arrow
from sqlalchemy.orm import Session

from dinofw.rest.base import BaseResource
from dinofw.rest.queries import NotificationQuery, HighlightStatus
from dinofw.utils.config import MessageEventType
from dinofw.utils.convert import stats_to_event_dict, to_int


class BroadcastResource(BaseResource):
    async def broadcast_event(self, query: NotificationQuery, db: Session) -> None:
        if query.event_type in MessageEventType.need_stats:
            await self.send_event_with_stats(query, db)
        else:
            self.send_other_event(query)

    async def send_event_with_stats(self, query: NotificationQuery, db: Session):
        user_id_to_stats = await self.get_stats_for(query.group_id, db)
        now_int = to_int(arrow.utcnow().int_timestamp)

        for user_group in query.notification:
            event = user_group.data.copy()
            event["event_type"] = query.event_type
            event["group_id"] = query.group_id

            # for 1-to-1 groups, the caller specifies user_ids, for many-to-many groups,
            # we send to all users in the group (unless user_ids is specified)
            users_to_notify = user_group.user_ids
            if not users_to_notify or not len(users_to_notify):
                users_to_notify = list(user_id_to_stats.keys())

            for user_id in users_to_notify:
                event_with_stats = event.copy()
                event_with_stats["stats"] = user_id_to_stats.get(user_id, dict())

                highlight_me = event_with_stats["stats"].get("highlight_time", self.long_ago.timestamp())
                highlight_receiver = event_with_stats["stats"].get("receiver_highlight_time", self.long_ago.timestamp())

                if highlight_me > now_int:
                    highlight_status = HighlightStatus.RECEIVER
                elif highlight_receiver > now_int:
                    highlight_status = HighlightStatus.SENDER
                else:
                    highlight_status = HighlightStatus.NONE

                event_with_stats["stats"]["highlight"] = highlight_status
                self.env.client_publisher.send_to_one(user_id, event_with_stats)

    def send_other_event(self, query: NotificationQuery):
        for user_group in query.notification:
            user_group.data["event_type"] = query.event_type
            user_group.data["group_id"] = query.group_id

            if user_group.topic and len(user_group.topic):
                self.env.client_publisher.send_to_topic(user_group.topic, user_group.data)
            else:
                for user_id in user_group.user_ids:
                    self.env.client_publisher.send_to_one(user_id, user_group.data)

    async def get_stats_for(self, group_id: str, db: Session):
        return {
            stat.user_id: stats_to_event_dict(stat)
            for stat in await self.env.db.get_all_user_stats_in_group(group_id, db, include_kicked=False)
        }
