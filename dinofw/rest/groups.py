import logging
from datetime import datetime as dt
from typing import List, Optional

import pytz
from sqlalchemy.orm import Session

from dinofw.rest.base import BaseResource
from dinofw.rest.models import AbstractQuery, UpdateUserGroupStats, ActionLog, GroupJoinTime
from dinofw.rest.models import AdminUpdateGroupQuery
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import Group
from dinofw.rest.models import GroupUsers
from dinofw.rest.models import Histories
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import SearchQuery
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UserGroupStats

logger = logging.getLogger(__name__)


class GroupResource(BaseResource):
    async def get_users_in_group(self, group_id: str, db: Session) -> Optional[GroupUsers]:
        group, users = self.env.db.get_users_in_group(group_id, db)

        if group is None:
            return None

        users = [
            GroupJoinTime(
                user_id=user_id,
                join_time=join_time,
            )
            for user_id, join_time in users.items()
        ]
        users.sort(key=lambda user: user.join_time, reverse=True)

        return GroupUsers(
            group_id=group_id,
            owner_id=group.owner_id,
            users=users
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
        user_stats_base = self.env.db.update_user_group_stats(group_id, user_id, query, db)

        return GroupResource.user_group_stats_base_to_user_group_stats(user_stats_base)

    async def create_new_group(self, user_id: int, query: CreateGroupQuery, db: Session) -> Group:
        group = self.env.db.create_group(user_id, query, db)
        user_ids = {user_id}

        if query.users is not None and query.users:
            user_ids.update(query.users)

        self.env.db.update_last_read_in_group_for_user(
            group.group_id,
            user_ids,
            group.created_at,
            db
        )

        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)

        self.env.storage.create_join_action_log(group.group_id, list(user_ids), now)

        return GroupResource.group_base_to_group(
            group,
            users=list(user_ids),
            last_read=group.created_at
        )

    async def admin_update_group_information(self, group_id, query: AdminUpdateGroupQuery, db: Session) -> bool:
        group_base = self.env.db.admin_update_group_information(group_id, query, db)

        if group_base is None:
            # TODO: return an error response instead
            return False

        return True

    async def update_group_information(self, user_id: int, group_id: str, query: UpdateGroupQuery, db: Session) -> None:
        group_base = self.env.db.update_group_information(group_id, query, db)

        if group_base is None:
            # TODO: return an error response instead
            return None

        return None

    async def join_group(self, group_id: str, user_id: int, db: Session) -> ActionLog:
        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)

        self.env.db.update_last_read_in_group_for_user(group_id, [user_id], now, db)
        action_log = self.env.storage.create_join_action_log(group_id, [user_id], now)

        return GroupResource.action_log_base_to_action_log(action_log[0])

    async def leave_group(self, group_id: str, user_id: int, db: Session) -> ActionLog:
        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)

        self.env.db.remove_last_read_in_group_for_user(group_id, [user_id], db)
        action_log = self.env.storage.create_leave_action_log(group_id, [user_id], now)

        return GroupResource.action_log_base_to_action_log(action_log[0])

    async def search(self, query: SearchQuery) -> List[Group]:
        return [self._group()]

    async def delete_one_group_for_user(self, user_id: int, group_id: str) -> None:
        pass

    async def delete_all_groups_for_user(self, user_id: int, group_id: str) -> None:
        pass
