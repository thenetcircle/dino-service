import random
from abc import ABC
from datetime import datetime
from typing import Optional, List
from uuid import uuid4 as uuid

import pytz

from dinofw.db.cassandra.schemas import MessageBase, ActionLogBase, JoinerBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase, GroupBase
from dinofw.rest.models import Group, Message, Joiner, AbstractQuery, UserGroupStats, ActionLog


class BaseResource(ABC):
    def _group(self, group_id=None):
        now = datetime.utcnow()
        now = now.replace(tzinfo=pytz.UTC)
        now = float(now.strftime("%s.%f"))

        if group_id is None:
            group_id = str(uuid())

        return Group(
            group_id=group_id,
            name="a group name",
            description="some description",
            status=0,
            group_type=0,
            created_at=now,
            updated_at=now,
            owner_id=0,
            group_meta=0,
            group_context="",
            last_message_overview="some text",
            last_message_user_id=0,
            last_message_time=now,
        )

    def _join(self, group_id, status=None):
        now = datetime.utcnow()
        now = now.replace(tzinfo=pytz.UTC)
        now = float(now.strftime("%s.%f"))

        if status is None:
            status = 0

        return Joiner(
            joined_id=int(random.random() * 1000000),
            group_id=group_id,
            inviter_id=int(random.random() * 1000000),
            created_at=now,
            status=status,
            invitation_context="",
        )

    def _message(self, group_id, user_id=None, message_id=None):
        now = datetime.utcnow()
        now = now.replace(tzinfo=pytz.UTC)
        now = float(now.strftime("%s.%f"))

        if user_id is None:
            user_id = int(random.random() * 1000000)

        if message_id is None:
            message_id = str(uuid())

        return Message(
            message_id=message_id,
            group_id=group_id,
            user_id=user_id,
            created_at=now,
            status=0,
            message_type=0,
            read_at=now,
            updated_at=now,
            last_action_log_id=0,
            removed_at=now,
            removed_by_user=0,
            message_payload="some message payload",
        )

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
    def joiner_base_to_joiner(join: JoinerBase) -> Joiner:
        join_dict = join.dict()

        join_dict["created_at"] = AbstractQuery.to_ts(join_dict["created_at"])

        return Joiner(**join_dict)

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
