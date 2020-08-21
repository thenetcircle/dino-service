import logging
from typing import List, Optional

import arrow
from sqlalchemy.orm import Session

from dinofw.rest.server.base import BaseResource
from dinofw.rest.server.models import AbstractQuery
from dinofw.rest.server.models import ActionLog
from dinofw.rest.server.models import CreateActionLogQuery
from dinofw.rest.server.models import CreateGroupQuery
from dinofw.rest.server.models import Group
from dinofw.rest.server.models import GroupJoinTime
from dinofw.rest.server.models import GroupQuery
from dinofw.rest.server.models import GroupUsers
from dinofw.rest.server.models import Histories
from dinofw.rest.server.models import MessageQuery
from dinofw.rest.server.models import PaginationQuery
from dinofw.rest.server.models import SearchQuery
from dinofw.rest.server.models import UpdateGroupQuery
from dinofw.rest.server.models import UpdateUserGroupStats
from dinofw.rest.server.models import UserGroupStats

logger = logging.getLogger(__name__)


class GroupResource(BaseResource):
    async def get_users_in_group(
        self, group_id: str, db: Session
    ) -> Optional[GroupUsers]:
        group, first_users, n_users = self.env.db.get_users_in_group(group_id, db)

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
            group_id=group_id,
            owner_id=group.owner_id,
            user_count=n_users,
            users=users,
        )

    async def get_group(self, group_id: str, db: Session) -> Optional[Group]:
        group, first_users, n_users = self.env.db.get_users_in_group(group_id, db)

        if group is None:
            # TODO: handle missing
            return None

        return GroupResource.group_base_to_group(
            group, users=first_users, last_read=None, user_count=n_users,
        )

    async def histories(self, group_id: str, user_id: int, query: MessageQuery, db: Session) -> Histories:
        user_stats = self.env.db.get_user_stats_in_group(group_id, user_id, db)

        if user_stats.hide:
            return Histories(messages=list(), action_logs=list(), last_reads=list())

        action_log = self.env.storage.get_action_log_in_group_for_user(group_id, user_stats, query)
        messages = self.env.storage.get_messages_in_group_for_user(group_id, user_stats, query)
        last_reads = self.env.db.get_last_reads_in_group(group_id, db)

        messages = [
            GroupResource.message_base_to_message(message) for message in messages
        ]
        action_log = [
            GroupResource.action_log_base_to_action_log(log) for log in action_log
        ]
        last_reads = [
            GroupResource.to_last_read(user_id, last_read) for user_id, last_read in last_reads.items()
        ]

        histories = Histories(
            messages=messages,
            action_logs=action_log,
            last_reads=last_reads,
        )

        return histories

    async def get_user_group_stats(
        self, group_id: str, user_id: int, db: Session
    ) -> Optional[UserGroupStats]:
        user_stats = self.env.db.get_user_stats_in_group(group_id, user_id, db)

        if user_stats is None:
            return None

        message_amount = self.env.storage.count_messages_in_group(group_id)

        last_sent = AbstractQuery.to_ts(user_stats.last_sent)
        delete_before = AbstractQuery.to_ts(user_stats.delete_before)
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
            delete_before=delete_before,
            hide=user_stats.hide,
            pin=user_stats.pin,
            bookmark=user_stats.bookmark,
        )

    async def update_user_group_stats(
        self, group_id: str, user_id: int, query: UpdateUserGroupStats, db: Session
    ) -> None:
        self.env.db.update_user_group_stats(group_id, user_id, query, db)

    async def create_action_logs(
            self,
            group_id: str,
            query: CreateActionLogQuery
    ) -> List[ActionLog]:
        logs = self.env.storage.create_action_logs(group_id, query)
        return [GroupResource.action_log_base_to_action_log(log) for log in logs]

    async def create_new_group(
        self, user_id: int, query: CreateGroupQuery, db: Session
    ) -> Group:
        group_base = self.env.db.create_group(user_id, query, db)

        now = arrow.utcnow().datetime
        now_ts = CreateGroupQuery.to_ts(now)

        users = {user_id: float(now_ts)}

        if query.users is not None and query.users:
            users.update({user_id: float(now_ts) for user_id in query.users})

        self.env.db.update_user_stats_on_join_or_create_group(
            group_base.group_id, users, now, db
        )

        group = GroupResource.group_base_to_group(
            group_base, users=users, last_read=now, user_count=len(users),
        )

        # notify users they're in a new group
        self.env.publisher.group_change(group_base, list(users.keys()))

        return group

    async def update_group_information(
        self, group_id: str, query: UpdateGroupQuery, db: Session
    ) -> None:
        group = self.env.db.update_group_information(group_id, query, db)

        user_ids_and_join_times = self.env.db.get_user_ids_and_join_time_in_group(group.group_id, db)
        user_ids = user_ids_and_join_times.keys()

        self.env.publisher.group_change(group, user_ids)

    async def join_group(self, group_id: str, user_id: int, db: Session) -> None:
        now = arrow.utcnow().datetime
        now_ts = AbstractQuery.to_ts(now)

        user_id_and_last_read = {user_id: float(now_ts)}

        self.env.db.set_group_updated_at(group_id, now, db)
        self.env.db.update_user_stats_on_join_or_create_group(
            group_id, user_id_and_last_read, now, db
        )

        user_ids_and_join_times = self.env.db.get_user_ids_and_join_time_in_group(group_id, db)
        user_ids_in_group = user_ids_and_join_times.keys()
        self.env.publisher.join(group_id, user_ids_in_group, user_id, now_ts)

    async def leave_group(self, group_id: str, user_id: int, db: Session) -> None:
        if not self.env.db.group_exists(group_id, db):
            return None

        now = arrow.utcnow().datetime
        now_ts = AbstractQuery.to_ts(now)

        self.env.db.remove_last_read_in_group_for_user(group_id, user_id, db)

        # shouldn't send this event to the guy who left, so get from db/cache after removing the leaver id
        user_ids_and_join_times = self.env.db.get_user_ids_and_join_time_in_group(group_id, db)
        user_ids_in_group = user_ids_and_join_times.keys()

        self.env.publisher.leave(group_id, user_ids_in_group, user_id, now_ts)

    async def search(self, query: SearchQuery) -> List[Group]:
        return list()  # TODO: implement

    async def delete_one_group_for_user(self, user_id: int, group_id: str) -> None:
        pass

    async def delete_all_groups_for_user(self, user_id: int, group_id: str) -> None:
        pass
