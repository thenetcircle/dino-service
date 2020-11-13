import logging
from typing import List
from typing import Optional

from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import AbstractQuery
from dinofw.rest.models import CreateActionLogQuery
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import Group
from dinofw.rest.models import GroupInfoQuery
from dinofw.rest.models import GroupJoinTime
from dinofw.rest.models import GroupUsers
from dinofw.rest.models import Histories
from dinofw.rest.models import Message
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import OneToOneStats
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UpdateUserGroupStats
from dinofw.rest.models import UserGroupStats
from dinofw.utils import utcnow_dt
from dinofw.utils import utcnow_ts
from dinofw.utils.decorators import time_method
from dinofw.utils.exceptions import NoSuchGroupException

logger = logging.getLogger(__name__)


class GroupResource(BaseResource):
    async def get_users_in_group(
        self, group_id: str, db: Session
    ) -> Optional[GroupUsers]:
        group, first_users, n_users = self.env.db.get_users_in_group(group_id, db)

        users = [
            GroupJoinTime(user_id=user_id, join_time=join_time,)
            for user_id, join_time in first_users.items()
        ]

        return GroupUsers(
            group_id=group_id, owner_id=group.owner_id, user_count=n_users, users=users,
        )

    async def get_group(self, group_id: str, query: GroupInfoQuery, db: Session) -> Optional[Group]:
        group, first_users, n_users = self.env.db.get_users_in_group(group_id, db)

        message_amount = -1
        if query.count_messages:
            message_amount = self.env.storage.count_messages_in_group_since(group_id, group.created_at)

        return GroupResource.group_base_to_group(
            group, users=first_users, user_count=n_users, message_amount=message_amount,
        )

    async def get_attachments_in_group_for_user(
        self, group_id: str, user_id: int, query: MessageQuery, db: Session
    ) -> List[Message]:
        user_stats = self.env.db.get_user_stats_in_group(group_id, user_id, db)
        attachments = self.env.storage.get_attachments_in_group_for_user(group_id, user_stats, query)

        return [
            GroupResource.message_base_to_message(attachment)
            for attachment in attachments
        ]

    def mark_all_as_read(self, user_id: int, db: Session) -> None:
        self.env.db.mark_all_groups_as_read(user_id, db)

    async def get_1v1_info(
        self, user_id_a: int, user_id_b: int, db: Session
    ) -> OneToOneStats:
        users = sorted([user_id_a, user_id_b])
        group = self.env.db.get_group_for_1to1(users[0], users[1], db)

        if group is None:
            raise NoSuchGroupException(",".join([str(user_id) for user_id in users]))

        group_id = group.group_id
        message_amount = await self.count_messages_in_group(group_id)
        users_and_join_time = self.env.db.get_user_ids_and_join_time_in_group(
            group_id, db
        )

        user_stats = [
            await self.get_user_group_stats(
                group_id, user_id, message_amount, db
            ) for user_id in users
        ]

        return OneToOneStats(
            stats=user_stats,
            group=GroupResource.group_base_to_group(
                group=group,
                users=users_and_join_time,
                user_count=len(users_and_join_time),
            ),
        )

    def set_last_updated_at_on_all_stats_related_to_user(self, user_id: int, db: Session) -> None:
        self.env.db.set_last_updated_at_on_all_stats_related_to_user(user_id, db)

    async def histories(
        self, group_id: str, user_id: int, query: MessageQuery, db: Session
    ) -> Histories:
        @time_method(logger, "histories()._user_stats()")
        def _user_stats():
            return self.env.db.get_user_stats_in_group(group_id, user_id, db)

        @time_method(logger, "histories()._get_messages()")
        def _get_messages():
            return [
                GroupResource.message_base_to_message(message)
                for message in self.env.storage.get_messages_in_group_for_user(
                    group_id, user_stats, query
                )
            ]

        @time_method(logger, "histories()._get_last_reads()")
        def _get_last_reads():
            return [
                GroupResource.to_last_read(this_user_id, last_read)
                for this_user_id, last_read in self.env.db.get_last_reads_in_group(
                    group_id, db
                ).items()
            ]

        user_stats = _user_stats()
        if user_stats.hide:
            return Histories(messages=list(), action_logs=list(), last_reads=list())

        messages = _get_messages()
        last_reads = _get_last_reads()

        if len(messages):
            self._user_opens_conversation(group_id, user_id, user_stats, db)

        return Histories(
            messages=messages,
            last_reads=last_reads,
        )

    async def count_messages_in_group(self, group_id: str) -> int:
        n_messages, until = self.env.cache.get_messages_in_group(group_id)

        if until is None:
            until = self.long_ago
            n_messages = 0
        else:
            until = AbstractQuery.to_dt(until)

        messages_since = self.env.storage.count_messages_in_group_since(group_id, until)
        total_messages = n_messages + messages_since
        now = utcnow_ts()

        self.env.cache.set_messages_in_group(group_id, total_messages, now)
        return total_messages

    async def get_user_group_stats(
        self, group_id: str, user_id: int, message_amount: int, db: Session
    ) -> Optional[UserGroupStats]:
        user_stats: UserGroupStatsBase = self.env.db.get_user_stats_in_group(
            group_id, user_id, db
        )

        if user_stats is None:
            return None

        delete_before = AbstractQuery.to_ts(user_stats.delete_before)
        last_updated_time = AbstractQuery.to_ts(user_stats.last_updated_time)
        last_sent = AbstractQuery.to_ts(user_stats.last_sent, allow_none=True)
        last_read = AbstractQuery.to_ts(user_stats.last_read, allow_none=True)
        first_sent = AbstractQuery.to_ts(user_stats.first_sent, allow_none=True)
        join_time = AbstractQuery.to_ts(user_stats.join_time, allow_none=True)

        unread_amount = self.env.storage.count_messages_in_group_since(
            group_id, user_stats.last_read
        )

        return UserGroupStats(
            user_id=user_id,
            group_id=group_id,
            message_amount=message_amount,
            unread=unread_amount,
            join_time=join_time,
            receiver_unread=-1,  # TODO: should be count for other user here as well?
            last_read_time=last_read,
            last_sent_time=last_sent,
            delete_before=delete_before,
            first_sent=first_sent,
            rating=user_stats.rating,
            hide=user_stats.hide,
            pin=user_stats.pin,
            bookmark=user_stats.bookmark,
            last_updated_time=last_updated_time,
        )

    async def update_user_group_stats(
        self, group_id: str, user_id: int, query: UpdateUserGroupStats, db: Session
    ) -> None:
        self.env.db.update_user_group_stats(group_id, user_id, query, db)

    async def create_action_logs(
        self, group_id: str, query: CreateActionLogQuery, db: Session
    ) -> List[Message]:
        logs = self.env.storage.create_action_logs(group_id, query)
        self._user_sends_action_logs(group_id, logs, db)

        return [GroupResource.message_base_to_message(log) for log in logs]

    async def create_new_group(
        self, user_id: int, query: CreateGroupQuery, db: Session
    ) -> Group:
        group_base = self.env.db.create_group(user_id, query, db)

        now = utcnow_dt()
        now_ts = CreateGroupQuery.to_ts(now)

        users = {user_id: float(now_ts)}

        if query.users is not None and query.users:
            users.update({user_id: float(now_ts) for user_id in query.users})

        self.env.db.update_user_stats_on_join_or_create_group(
            group_base.group_id, users, now, db
        )

        group = GroupResource.group_base_to_group(
            group=group_base, users=users, user_count=len(users),
        )

        # notify users they're in a new group
        self.env.client_publisher.group_change(group_base, list(users.keys()))

        return group

    async def update_group_information(
        self, group_id: str, query: UpdateGroupQuery, db: Session
    ) -> None:
        group = self.env.db.update_group_information(group_id, query, db)
        self.env.db.set_last_updated_at_for_all_in_group(group_id, db)

        user_ids_and_join_times = self.env.db.get_user_ids_and_join_time_in_group(
            group.group_id, db
        )
        user_ids = user_ids_and_join_times.keys()

        self.env.client_publisher.group_change(group, user_ids)

    async def join_group(self, group_id: str, user_id: int, db: Session) -> None:
        now = utcnow_dt()
        now_ts = AbstractQuery.to_ts(now)

        user_id_and_last_read = {user_id: float(now_ts)}

        self.env.db.set_group_updated_at(group_id, now, db)
        self.env.db.update_user_stats_on_join_or_create_group(
            group_id, user_id_and_last_read, now, db
        )

        user_ids_and_join_times = self.env.db.get_user_ids_and_join_time_in_group(
            group_id, db
        )
        user_ids_in_group = user_ids_and_join_times.keys()
        self.env.client_publisher.join(group_id, user_ids_in_group, user_id, now_ts)

    def leave_group(self, group_id: str, user_id: int, db: Session) -> None:
        now = utcnow_dt()
        now_ts = AbstractQuery.to_ts(now)

        self.env.db.remove_last_read_in_group_for_user(group_id, user_id, db)

        # shouldn't send this event to the guy who left, so get from db/cache after removing the leaver id
        user_ids_and_join_times = self.env.db.get_user_ids_and_join_time_in_group(
            group_id, db
        )

        # if it's the last user we don't need to publish anything
        if len(user_ids_and_join_times):
            user_ids_in_group = user_ids_and_join_times.keys()
            self.env.client_publisher.leave(group_id, user_ids_in_group, user_id, now_ts)

    def delete_attachments_in_group_for_user(self, group_id: str, user_id: int, db: Session) -> None:
        group = self.env.db.get_group_from_id(group_id, db)

        attachments = self.env.storage.delete_attachments(
            group_id, group.created_at, user_id
        )

        now = utcnow_ts()
        user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db).keys()

        if len(user_ids):
            for publisher in [self.env.client_publisher, self.env.server_publisher]:
                publisher.delete_attachments(group_id, attachments, user_ids, now)

            # TODO: how to tell apps an attachment was deleted? <-- update: create action log on deletions
            # self.env.db.update_group_updated_at ?

    def delete_all_groups_for_user(self, user_id: int, db: Session) -> None:
        group_ids = self.env.db.get_all_group_ids_for_user(user_id, db)

        # TODO: this is async, but check how long time this would take for like 5-10k groups
        for group_id in group_ids:
            self.leave_group(group_id, user_id, db)
