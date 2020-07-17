from abc import ABC
from datetime import datetime
from typing import Optional, List

from dinofw.db.cassandra.schemas import MessageBase, ActionLogBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase, GroupBase
from dinofw.rest.models import Group, Message, AbstractQuery, UserGroupStats, ActionLog


class BaseResource(ABC):
    @staticmethod
    def message_base_to_message(message: MessageBase) -> Message:
        message_dict = message.dict()

        message_dict["removed_at"] = AbstractQuery.to_ts(message_dict["removed_at"], allow_none=True)
        message_dict["updated_at"] = AbstractQuery.to_ts(message_dict["updated_at"], allow_none=True)
        message_dict["created_at"] = AbstractQuery.to_ts(message_dict["created_at"], allow_none=True)

        return Message(**message_dict)

    @staticmethod
    def group_base_to_group(group: GroupBase, users: List[int], last_read: Optional[datetime]) -> Group:
        group_dict = group.dict()

        group_dict["updated_at"] = AbstractQuery.to_ts(group_dict["updated_at"], allow_none=True)
        group_dict["created_at"] = AbstractQuery.to_ts(group_dict["created_at"])
        group_dict["last_message_time"] = AbstractQuery.to_ts(group_dict["last_message_time"])
        group_dict["last_read"] = AbstractQuery.to_ts(last_read)
        group_dict["users"] = users

        return Group(**group_dict)

    @staticmethod
    def user_group_stats_base_to_user_group_stats(user_stats: UserGroupStatsBase):
        stats_dict = user_stats.dict()

        stats_dict["last_read"] = AbstractQuery.to_ts(stats_dict["last_read"])
        stats_dict["last_sent"] = AbstractQuery.to_ts(stats_dict["last_sent"])
        stats_dict["hide_before"] = AbstractQuery.to_ts(stats_dict["hide_before"])

        return UserGroupStats(**stats_dict)

    @staticmethod
    def action_log_base_to_action_log(action_log: ActionLogBase) -> ActionLog:
        action_dict = action_log.dict()

        action_dict["created_at"] = AbstractQuery.to_ts(action_dict["created_at"])

        return ActionLog(**action_dict)
