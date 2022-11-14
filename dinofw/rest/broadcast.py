import arrow
from sqlalchemy.orm import Session

from dinofw.endpoint import EventTypes
from dinofw.rest.base import BaseResource
from dinofw.rest.queries import NotificationQuery, EventType, HighlightStatus
from dinofw.utils.convert import stats_to_event_dict, to_int


class BroadcastResource(BaseResource):
    async def broadcast_event(self, query: NotificationQuery, db: Session) -> None:
        if query.event_type == EventType.message:
            self.send_message_event(query, db)
        else:
            self.send_other_event(query)

    def send_message_event(self, query: NotificationQuery, db: Session):
        user_id_to_stats = self.get_stats_for(query.group_id, db)

        for user_group in query.notification:
            event = user_group.data.copy()
            event["event_type"] = EventTypes.MESSAGE
            event["group_id"] = query.group_id

            # FE needs to compare highlight time with current utc server time
            event["published"] = to_int(arrow.utcnow().timestamp())

            for user_id in user_group.user_ids:
                event_with_stats = event.copy()
                event_with_stats["stats"] = user_id_to_stats.get(user_id, dict())

                highlight_me = event_with_stats["stats"]["highlight_time"]
                highlight_receiver = event_with_stats["stats"]["receiver_highlight_time"]

                if highlight_me > now_int:
                    highlight_status = 1  # HIGHLIGHT_STATUS_RECEIVER
                elif highlight_receiver > now_int:
                    highlight_status = 2  # HIGHLIGHT_STATUS_SENDER
                else:
                    highlight_status = 0  # HIGHLIGHT_STATUS_NONE

                event_with_stats["stats"]["highlight"] = highlight_status
                self.env.client_publisher.send_to_one(user_id, event_with_stats)

    def send_other_event(self, query: NotificationQuery):
        for user_group in query.notification:
            user_group.data["event_type"] = query.event_type
            user_group.data["group_id"] = query.group_id

            for user_id in user_group.user_ids:
                self.env.client_publisher.send_to_one(user_id, user_group.data)

    def get_stats_for(self, group_id: str, db: Session):
        return {
            stat.user_id: stats_to_event_dict(stat)
            for stat in self.env.db.get_all_user_stats_in_group(group_id, db)
        }
