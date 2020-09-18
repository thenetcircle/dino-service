import logging
from abc import ABC
from datetime import datetime as dt
from typing import Dict, Any, List

import arrow
import pytz

from dinofw.db.rdbms.schemas import UserGroupStatsBase, GroupBase
from dinofw.db.storage.schemas import MessageBase, ActionLogBase, AttachmentBase
from dinofw.rest.models import AbstractQuery, UserGroup, Attachment
from dinofw.rest.models import ActionLog
from dinofw.rest.models import Group
from dinofw.rest.models import GroupJoinTime
from dinofw.rest.models import GroupLastRead
from dinofw.rest.models import Message
from dinofw.rest.models import UserGroupStats


class BaseResource(ABC):
    def __init__(self, env):
        self.env = env

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = dt.utcfromtimestamp(beginning_of_1995)
        self.long_ago = self.long_ago.replace(tzinfo=pytz.UTC)

        self.logger = logging.getLogger(__name__)

    def _user_opens_conversation(self, group_id: str, user_id: int, db):
        """
        update database and cache with everything related to opening a conversation
        """
        now = arrow.utcnow().datetime
        user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db)

        self.env.db.update_last_read_and_highlight_in_group_for_user(
            group_id, user_id, now, db
        )
        self.env.publisher.read(group_id, user_id, user_ids)
        self.env.cache.set_unread_in_group(group_id, user_id, 0)

    def _user_sends_a_message(
        self, group_id: str, user_id: int, message: MessageBase, db
    ):
        """
        update database and cache with everything related to sending a message
        """
        # cassandra DT is different from python DT
        now = arrow.utcnow().datetime

        self.env.db.update_group_new_message(message, now, db)
        self.env.db.update_last_read_and_sent_in_group_for_user(
            group_id, user_id, now, db
        )

        user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db)
        self.env.publisher.message(message, user_ids)

        # don't increase unread for the sender
        del user_ids[user_id]
        self.env.cache.increase_unread_in_group_for(group_id, user_ids)

    def _user_sends_an_attachment(self, group_id: str, attachment: AttachmentBase, db):
        user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db)
        self.env.publisher.attachment(attachment, user_ids)

    @staticmethod
    def message_base_to_message(message: MessageBase) -> Message:
        message_dict = message.dict()

        message_dict["updated_at"] = AbstractQuery.to_ts(
            message_dict["updated_at"], allow_none=True
        )
        message_dict["created_at"] = AbstractQuery.to_ts(
            message_dict["created_at"], allow_none=True
        )

        return Message(**message_dict)

    @staticmethod
    def attachment_base_to_attachment(attachment: MessageBase) -> Attachment:
        attachment_dict = attachment.dict()

        attachment_dict["updated_at"] = AbstractQuery.to_ts(
            attachment_dict["updated_at"], allow_none=True
        )
        attachment_dict["created_at"] = AbstractQuery.to_ts(
            attachment_dict["created_at"], allow_none=True
        )

        return Attachment(**attachment_dict)

    @staticmethod
    def group_base_to_group(
        group: GroupBase, users: Dict[int, float], user_count: int,
    ) -> Group:
        group_dict = group.dict()

        users = [
            GroupJoinTime(user_id=user_id, join_time=join_time,)
            for user_id, join_time in users.items()
        ]
        users.sort(key=lambda user: user.join_time, reverse=True)

        group_dict["updated_at"] = AbstractQuery.to_ts(
            group_dict["updated_at"], allow_none=True
        )
        group_dict["created_at"] = AbstractQuery.to_ts(group_dict["created_at"])
        group_dict["last_message_time"] = AbstractQuery.to_ts(
            group_dict["last_message_time"]
        )
        group_dict["users"] = users
        group_dict["user_count"] = user_count

        return Group(**group_dict)

    @staticmethod
    def group_base_to_user_group(
        group_base: GroupBase,
        stats_base: UserGroupStatsBase,
        users: Dict[int, float],
        user_count: int,
        unread_count: int,
    ) -> UserGroup:
        group = BaseResource.group_base_to_group(group_base, users, user_count)

        stats_dict = stats_base.__dict__
        stats_dict["unread_amount"] = unread_count

        stats_dict["last_read_time"] = AbstractQuery.to_ts(stats_base.last_read)
        stats_dict["last_sent_time"] = AbstractQuery.to_ts(stats_base.last_sent)
        stats_dict["delete_before"] = AbstractQuery.to_ts(stats_base.delete_before)
        stats_dict["highlight_time"] = AbstractQuery.to_ts(
            stats_base.highlight_time, allow_none=True
        )
        stats_dict["first_sent"] = AbstractQuery.to_ts(
            stats_base.first_sent, allow_none=True
        )
        stats_dict["last_updated_time"] = AbstractQuery.to_ts(
            stats_base.last_updated_time
        )

        stats = UserGroupStats(**stats_dict)

        return UserGroup(group=group, stats=stats,)

    @staticmethod
    def user_group_stats_base_to_user_group_stats(user_stats: UserGroupStatsBase):
        stats_dict = user_stats.dict()

        stats_dict["last_read"] = AbstractQuery.to_ts(stats_dict["last_read"])
        stats_dict["last_sent"] = AbstractQuery.to_ts(stats_dict["last_sent"])
        stats_dict["delete_before"] = AbstractQuery.to_ts(stats_dict["delete_before"])

        return UserGroupStats(**stats_dict)

    @staticmethod
    def action_log_base_to_action_log(action_log: ActionLogBase) -> ActionLog:
        action_dict = action_log.dict()

        action_dict["created_at"] = AbstractQuery.to_ts(action_dict["created_at"])

        return ActionLog(**action_dict)

    @staticmethod
    def to_last_read(user_id: int, last_read: float) -> GroupLastRead:
        return GroupLastRead(user_id=user_id, last_read=last_read)
