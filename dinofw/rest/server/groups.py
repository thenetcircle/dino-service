import logging
from datetime import datetime as dt
from typing import List, Optional

import pytz
from sqlalchemy.orm import Session

from dinofw.rest.server.base import BaseResource
from dinofw.rest.server.models import (
    AbstractQuery,
    UpdateUserGroupStats,
    ActionLog,
    GroupJoinTime,
    GroupQuery,
)
from dinofw.rest.server.models import AdminUpdateGroupQuery
from dinofw.rest.server.models import CreateGroupQuery
from dinofw.rest.server.models import Group
from dinofw.rest.server.models import GroupUsers
from dinofw.rest.server.models import Histories
from dinofw.rest.server.models import MessageQuery
from dinofw.rest.server.models import SearchQuery
from dinofw.rest.server.models import UpdateGroupQuery
from dinofw.rest.server.models import UserGroupStats

logger = logging.getLogger(__name__)


class GroupResource(BaseResource):
    async def get_users_in_group(
        self, group_id: str, db: Session
    ) -> Optional[GroupUsers]:
        # TODO: this should have pagination

        # limit list of users/join times to first 50
        query = GroupQuery(per_page=50)

        group, first_users, n_users = self.env.db.get_users_in_group(
            group_id, query, db
        )

        if group is None:
            return None

        users = [
            GroupJoinTime(
                user_id=user_id,
                join_time=join_time,
            )
            for user_id, join_time in first_users.items()
        ]

        return GroupUsers(
            group_id=group_id, owner_id=group.owner_id, user_count=n_users, users=users,
        )

    async def get_group(self, group_id: str, db: Session) -> Optional[Group]:
        # limit list of users/join times to first 50
        query = GroupQuery(per_page=50)

        group, first_users, n_users = self.env.db.get_users_in_group(
            group_id, query, db
        )

        if group is None:
            # TODO: handle missing
            return None

        return GroupResource.group_base_to_group(
            group, users=first_users, last_read=None, user_count=n_users,
        )

    async def histories(self, group_id: str, query: MessageQuery) -> Histories:
        action_log = self.env.storage.get_action_log_in_group(group_id, query)
        messages = self.env.storage.get_messages_in_group(group_id, query)

        messages = [
            GroupResource.message_base_to_message(message) for message in messages
        ]
        action_log = [
            GroupResource.action_log_base_to_action_log(log) for log in action_log
        ]

        histories = Histories(
            messages=messages,
            action_logs=action_log
        )

        return histories

    async def get_user_group_stats(
        self, group_id: str, user_id: int, db: Session
    ) -> UserGroupStats:
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

            unread_amount = self.env.storage.count_messages_in_group_since(
                group_id, user_stats.last_read
            )

        return UserGroupStats(
            user_id=user_id,
            group_id=group_id,
            message_amount=message_amount,
            unread_amount=unread_amount,
            last_read_time=last_read,
            last_send_time=last_sent,
            hide_before=hide_before,
        )

    async def update_user_group_stats(
        self, group_id: str, user_id: int, query: UpdateUserGroupStats, db: Session
    ) -> None:
        self.env.db.update_user_group_stats(group_id, user_id, query, db)

    async def create_new_group(
        self, user_id: int, query: CreateGroupQuery, db: Session
    ) -> Group:
        group = self.env.db.create_group(user_id, query, db)

        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)
        now_ts = CreateGroupQuery.to_ts(now)

        users = {user_id: now_ts}

        if query.users is not None and query.users:
            users.update({user_id: now_ts for user_id in query.users})

        self.env.db.update_last_read_in_group_for_user(
            group.group_id, users, now, db
        )

        self.env.storage.create_join_action_log(group.group_id, users, now)

        return GroupResource.group_base_to_group(
            group, users=users, last_read=now, user_count=len(users),
        )

    async def admin_update_group_information(
        self, group_id, query: AdminUpdateGroupQuery, db: Session
    ) -> bool:
        group_base = self.env.db.admin_update_group_information(group_id, query, db)

        if group_base is None:
            # TODO: return an error response instead
            return False

        return True

    async def update_group_information(
        self, user_id: int, group_id: str, query: UpdateGroupQuery, db: Session
    ) -> None:
        group_base = self.env.db.update_group_information(group_id, query, db)

        if group_base is None:
            # TODO: return an error response instead
            return None

        return None

    async def join_group(self, group_id: str, user_id: int, db: Session) -> ActionLog:
        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)

        user_id_and_last_read = {user_id: now}

        self.env.db.update_last_read_in_group_for_user(
            group_id, user_id_and_last_read, now, db
        )
        action_log = self.env.storage.create_join_action_log(
            group_id, user_id_and_last_read, now
        )

        return GroupResource.action_log_base_to_action_log(action_log[0])

    async def leave_group(self, group_id: str, user_id: int, db: Session) -> ActionLog:
        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)

        self.env.db.remove_last_read_in_group_for_user(group_id, [user_id], db)
        action_log = self.env.storage.create_leave_action_log(group_id, [user_id], now)

        return GroupResource.action_log_base_to_action_log(action_log[0])

    async def search(self, query: SearchQuery) -> List[Group]:
        return list()  # TODO: implement

    async def delete_one_group_for_user(self, user_id: int, group_id: str) -> None:
        pass

    async def delete_all_groups_for_user(self, user_id: int, group_id: str) -> None:
        pass
