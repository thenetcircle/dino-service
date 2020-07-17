import logging
from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import Session

from dinofw.db.cassandra.schemas import JoinerBase, MessageBase, ActionLogBase
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import AbstractQuery, UpdateUserGroupStats, ActionLog
from dinofw.rest.models import AdminUpdateGroupQuery
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import Group
from dinofw.rest.models import GroupJoinQuery
from dinofw.rest.models import GroupJoinerQuery
from dinofw.rest.models import GroupUsers
from dinofw.rest.models import Histories
from dinofw.rest.models import Joiner
from dinofw.rest.models import JoinerUpdateQuery
from dinofw.rest.models import Message
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import SearchQuery
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UserGroupStats

logger = logging.getLogger(__name__)


class GroupResource(BaseResource):
    def __init__(self, env):
        self.env = env

    async def get_users_in_group(self, group_id: str, db: Session) -> GroupUsers:
        group, user_ids = self.env.db.get_users_in_group(group_id, db)

        return GroupUsers(
            group_id=group_id,
            owner_id=group.owner_id,
            users=user_ids
        )

    async def get_group(self, group_id: str, db: Session):
        group, user_ids = self.env.db.get_users_in_group(group_id, db)

        return GroupResource.group_base_to_group(
            group,
            users=user_ids,
            last_read=None
        )

    async def histories(
        self, group_id: str, query: MessageQuery
    ) -> List[Histories]:
        action_log = self.env.storage.get_action_log_in_group(group_id, query)
        messages = self.env.storage.get_messages_in_group(group_id, query)

        messages = [GroupResource.message_base_to_message(message) for message in messages]
        action_log = [GroupResource.action_log_base_to_action_log(log) for log in action_log]

        histories = [
            Histories(messages=messages),
            Histories(action_logs=action_log),
        ]

        return histories

    async def message(self, group_id: str, user_id: int, message_id: str) -> Message:
        return self._message(group_id, user_id, message_id)

    async def get_user_group_stats(self, group_id: str, user_id: int, db: Session) -> UserGroupStats:
        user_stats = self.env.db.get_user_stats_in_group(group_id, user_id, db)
        message_amount = self.env.storage.count_messages_in_group(group_id)

        last_sent = 0
        last_read = 0
        hide_before = 0
        unread_amount = message_amount

        if user_stats is not None:
            last_sent = AbstractQuery.to_ts(user_stats.last_sent)
            hide_before = AbstractQuery.to_ts(user_stats.hide_before)
            last_read = AbstractQuery.to_ts(user_stats.last_read)

            unread_amount = self.env.storage.count_messages_in_group_since(group_id, user_stats.last_read)

        return UserGroupStats(
            user_id=user_id,
            group_id=group_id,
            message_amount=message_amount,
            unread_amount=unread_amount,
            last_read_time=last_read,
            last_send_time=last_sent,
            hide_before=hide_before,
        )

    async def update_user_group_stats(self, group_id: str, user_id: int, query: UpdateUserGroupStats, db: Session):
        user_stats_base = self.env.db.update_user_stats_in_group(group_id, user_id, query, db)

        return GroupResource.user_group_stats_base_to_user_group_stats(user_stats_base)

    async def create_new_group(self, user_id: int, query: CreateGroupQuery, db: Session) -> Group:
        group = self.env.db.create_group(user_id, query, db)

        self.env.db.update_last_read_in_group_for_user(
            user_id,
            group.group_id,
            group.created_at,
            db
        )

        return GroupResource.group_base_to_group(
            group,
            users=[user_id],
            last_read=group.created_at
        )

    async def get_join_requests(
        self, group_id: str, query: GroupJoinerQuery
    ) -> List[Joiner]:
        joins = self.env.storage.get_group_joins_for_status(group_id, query.status)

        return [GroupResource.joiner_base_to_joiner(join) for join in joins]

    async def save_join_request(
        self, group_id: str, query: GroupJoinQuery
    ) -> Joiner:
        join = self.env.storage.save_group_join_request(group_id, query)

        return GroupResource.joiner_base_to_joiner(join)

    async def get_join_details(
        self, user_id: int, group_id: str, joiner_id: int
    ) -> Joiner:
        join = self.env.storage.get_group_join_for_user(group_id, joiner_id)

        return GroupResource.joiner_base_to_joiner(join)

    async def admin_update_group_information(self, group_id, query: AdminUpdateGroupQuery, db: Session) -> bool:
        group_base = self.env.db.admin_update_group_information(group_id, query, db)

        if group_base is None:
            # TODO: return an error response instead
            return False

        return True

    async def update_group_information(self, user_id: int, group_id: str, query: UpdateGroupQuery, db: Session) -> bool:
        group_base = self.env.db.update_group_information(group_id, user_id, query, db)

        if group_base is None:
            # TODO: return an error response instead
            return False

        return True

    async def delete_join_request(self, group_id: str, joiner_id: int) -> None:
        self.env.storage.delete_join_request(group_id, joiner_id)

    async def update_join_request(self, group_id: str, joiner_id: int, query: JoinerUpdateQuery) -> bool:
        joiner_base = self.env.storage.update_join_request(group_id, joiner_id, query)

        if joiner_base is None:
            # TODO: return an error response instead
            return False

        return True

    async def search(self, query: SearchQuery) -> List[Group]:
        return [self._group()]

    async def hide_histories_for_user(
        self, group_id: str, user_id: int, query: MessageQuery
    ):
        pass

    async def delete_one_group_for_user(self, user_id: int, group_id: str) -> None:
        pass

    async def delete_all_groups_for_user(self, user_id: int, group_id: str) -> None:
        pass

    @staticmethod
    def group_base_to_group(group: GroupBase, users: List[int], last_read: Optional[datetime]) -> Group:
        group_dict = group.dict()

        group_dict["updated_at"] = CreateGroupQuery.to_ts(group_dict["updated_at"])
        group_dict["created_at"] = CreateGroupQuery.to_ts(group_dict["created_at"])
        group_dict["last_message_time"] = CreateGroupQuery.to_ts(group_dict["last_message_time"])
        group_dict["last_read"] = CreateGroupQuery.to_ts(last_read)
        group_dict["users"] = users

        return Group(**group_dict)

    @staticmethod
    def joiner_base_to_joiner(join: JoinerBase) -> Joiner:
        join_dict = join.dict()

        join_dict["created_at"] = GroupJoinerQuery.to_ts(join_dict["created_at"])

        return Joiner(**join_dict)

    @staticmethod
    def user_group_stats_base_to_user_group_stats(user_stats: UserGroupStatsBase):
        stats_dict = user_stats.dict()

        stats_dict["last_read"] = AbstractQuery.to_ts(stats_dict["last_read"])
        stats_dict["last_sent"] = AbstractQuery.to_ts(stats_dict["last_sent"])
        stats_dict["hide_before"] = AbstractQuery.to_ts(stats_dict["hide_before"])

        return UserGroupStats(**stats_dict)

    @staticmethod
    def message_base_to_message(message: MessageBase) -> Message:
        message_dict = message.dict()

        message_dict["created_at"] = AbstractQuery.to_ts(message_dict["created_at"])
        message_dict["updated_at"] = AbstractQuery.to_ts(message_dict["updated_at"])
        message_dict["removed_at"] = AbstractQuery.to_ts(message_dict["removed_at"])

        return Message(**message_dict)

    @staticmethod
    def action_log_base_to_action_log(action_log: ActionLogBase) -> ActionLog:
        action_dict = action_log.dict()

        action_dict["created_at"] = AbstractQuery.to_ts(action_dict["created_at"])

        return ActionLog(**action_dict)
