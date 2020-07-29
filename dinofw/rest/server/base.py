from abc import ABC
from datetime import datetime as dt
from typing import Optional, Dict

import pytz

from dinofw.db.cassandra.schemas import MessageBase, ActionLogBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase, GroupBase
from dinofw.rest.server.models import (
    Group,
    Message,
    AbstractQuery,
    UserGroupStats,
    ActionLog,
    GroupJoinTime,
)


class BaseResource(ABC):
    def __init__(self, env):
        self.env = env

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = dt.utcfromtimestamp(beginning_of_1995)
        self.long_ago = self.long_ago.replace(tzinfo=pytz.UTC)

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
    def group_base_to_group(
        group: GroupBase,
        users: Dict[int, float],
        last_read: Optional[dt],
        user_count: int,
    ) -> Group:
        group_dict = group.dict()

        users = [
            GroupJoinTime(
                user_id=user_id,
                join_time=join_time,
            )
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
        group_dict["last_read"] = AbstractQuery.to_ts(last_read)
        group_dict["users"] = users
        group_dict["user_count"] = user_count

        return Group(**group_dict)

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
