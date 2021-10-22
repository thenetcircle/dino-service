import itertools
from typing import List
from typing import Optional

from loguru import logger
from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import Group
from dinofw.rest.models import GroupJoinTime
from dinofw.rest.models import GroupUsers
from dinofw.rest.models import Histories
from dinofw.rest.models import Message
from dinofw.rest.models import OneToOneStats
from dinofw.rest.models import UserGroupStats
from dinofw.rest.queries import AbstractQuery
from dinofw.rest.queries import CreateActionLogQuery
from dinofw.rest.queries import CreateGroupQuery
from dinofw.rest.queries import GroupInfoQuery
from dinofw.rest.queries import JoinGroupQuery
from dinofw.rest.queries import MessageQuery
from dinofw.rest.queries import UpdateGroupQuery
from dinofw.rest.queries import UpdateUserGroupStats
from dinofw.utils import utcnow_dt
from dinofw.utils import utcnow_ts
from dinofw.utils.decorators import time_method
from dinofw.utils.exceptions import NoSuchGroupException, InvalidRangeException


class GroupResource(BaseResource):
    async def get_users_in_group(
        self, group_id: str, db: Session
    ) -> Optional[GroupUsers]:
        """
        TODO: remove this api, not needed since we have POST /v1/groups/{group_id}  (Get Group Information)
        """
        group, first_users, n_users = self.env.db.get_users_in_group(group_id, db)

        users = [
            GroupJoinTime(user_id=user_id, join_time=join_time,)
            for user_id, join_time in first_users.items()
        ]

        return GroupUsers(
            group_id=group_id, owner_id=group.owner_id, user_count=n_users, users=users,
        )

    async def get_group(
            self,
            group_id: str,
            query: GroupInfoQuery,
            db: Session,
            message_amount: int = -1
    ) -> Optional[Group]:
        group, first_users, n_users = self.env.db.get_users_in_group(group_id, db)

        if query.count_messages:
            message_amount = self.env.storage.count_messages_in_group_since(group_id, group.created_at)

        return GroupResource.group_base_to_group(
            group, users=first_users, user_count=n_users, message_amount=message_amount,
        )

    async def get_attachments_in_group_for_user(
        self, group_id: str, user_id: int, query: MessageQuery, db: Session
    ) -> List[Message]:
        if query.since is None and query.until is None:
            raise InvalidRangeException("both 'since' and 'until' was empty, need one")
        if query.since is not None and query.until is not None:
            raise InvalidRangeException("only one of parameters 'since' and 'until' can be used at the same time")

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

        group_id = group.group_id
        message_amount = await self.count_messages_in_group(group_id)
        users_and_join_time = self.env.db.get_user_ids_and_join_time_in_group(
            group_id, db
        )

        user_stats = [
            await self.get_user_group_stats(
                group_id, user_id, db
            ) for user_id in users
        ]

        user_a: Optional[UserGroupStats] = user_stats[0]
        user_b: Optional[UserGroupStats] = user_stats[1]

        if user_a is not None and user_b is not None:
            for this_user, that_user in itertools.permutations([user_a, user_b]):
                this_user.receiver_unread = that_user.unread
                this_user.receiver_hide = that_user.hide
                this_user.receiver_deleted = that_user.deleted
                this_user.receiver_highlight_time = that_user.highlight_time
                this_user.receiver_delete_before = that_user.delete_before

        return OneToOneStats(
            stats=user_stats,
            group=GroupResource.group_base_to_group(
                group=group,
                users=users_and_join_time,
                user_count=len(users_and_join_time),
                message_amount=message_amount
            ),
        )

    def set_last_updated_at_on_all_stats_related_to_user(self, user_id: int, db: Session) -> None:
        self.env.db.set_last_updated_at_on_all_stats_related_to_user(user_id, db)

    async def histories(
        self, group_id: str, user_id: int, query: MessageQuery, db: Session
    ) -> Histories:
        @time_method(logger, "histories().user_stats()")
        def get_user_stats():
            return self.env.db.get_user_stats_in_group(group_id, user_id, db)

        @time_method(logger, "histories().get_messages()")
        def get_messages():
            return [
                GroupResource.message_base_to_message(message)
                for message in self.env.storage.get_messages_in_group_for_user(
                    group_id, user_stats, query
                )
            ]

        @time_method(logger, "histories().get_last_reads()")
        def get_last_reads():
            return [
                GroupResource.to_last_read(this_user_id, last_read)
                for this_user_id, last_read in self.env.db.get_last_reads_in_group(
                    group_id, db
                ).items()
            ]

        if query.since is None and query.until is None:
            raise InvalidRangeException("both 'since' and 'until' was empty, need one")
        if query.since is not None and query.until is not None:
            raise InvalidRangeException("only one of parameters 'since' and 'until' can be used at the same time")

        user_stats = get_user_stats()
        messages = get_messages()

        # history api can be called by the admin interface, in which case we don't want to change read status
        if query.admin_id is None or query.admin_id == 0:
            self._user_opens_conversation(group_id, user_id, user_stats, db)

        last_reads = get_last_reads()

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
        self, group_id: str, user_id: int, db: Session
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
        highlight_time = AbstractQuery.to_ts(user_stats.highlight_time, allow_none=True)

        # try using the counter column on the stats table instead of actually counting
        """
        unread_amount = self.env.storage.count_messages_in_group_since(
            group_id, user_stats.last_read
        )
        """

        return UserGroupStats(
            user_id=user_id,
            group_id=group_id,
            unread=user_stats.unread_count,
            join_time=join_time,
            receiver_unread=-1,  # TODO: should be count for other user here as well?
            last_read_time=last_read,
            last_sent_time=last_sent,
            delete_before=delete_before,
            first_sent=first_sent,
            rating=user_stats.rating,
            highlight_time=highlight_time,
            hide=user_stats.hide,
            pin=user_stats.pin,
            deleted=user_stats.deleted,
            bookmark=user_stats.bookmark,
            last_updated_time=last_updated_time,
        )

    async def update_user_group_stats(
        self, group_id: str, user_id: int, query: UpdateUserGroupStats, db: Session
    ) -> None:
        self.env.db.update_user_group_stats(group_id, user_id, query, db)
        self.create_action_log(query.action_log, db, user_id=user_id, group_id=group_id)

    async def create_new_group(
        self, user_id: int, query: CreateGroupQuery, db: Session
    ) -> Group:
        now = utcnow_dt()
        now_ts = CreateGroupQuery.to_ts(now)

        group_base = self.env.db.create_group(user_id, query, now, db)
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

        self.create_action_log(query.action_log, db, group_id=group_id)
        self.env.client_publisher.group_change(group, user_ids)

    async def join_group(self, group_id: str, query: JoinGroupQuery, db: Session) -> None:
        now = utcnow_dt()
        now_ts = AbstractQuery.to_ts(now)

        user_ids_and_last_read = {
            user_id: float(now_ts)
            for user_id in query.users
        }

        self.env.db.set_group_updated_at(group_id, now, db)
        self.env.db.update_user_stats_on_join_or_create_group(
            group_id, user_ids_and_last_read, now, db
        )

        self.create_action_log(query.action_log, db, group_id=group_id)

    def leave_group(self, group_id: str, user_id: int, query: CreateActionLogQuery, db: Session) -> None:
        self.env.db.remove_last_read_in_group_for_user(group_id, user_id, db)
        self.create_action_log(query.action_log, db, user_id=user_id, group_id=group_id)

    def delete_attachments_in_group_for_user(
            self,
            group_id: str,
            user_id: int,
            query: CreateActionLogQuery,
            db: Session
    ) -> None:
        group = self.env.db.get_group_from_id(group_id, db)

        attachments = self.env.storage.delete_attachments(
            group_id, group.created_at, user_id
        )

        now = utcnow_ts()
        user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db).keys()

        self.env.server_publisher.delete_attachments(group_id, attachments, user_ids, now)
        self.create_action_log(query.action_log, db, user_id=user_id, group_id=group_id)

    def delete_all_groups_for_user(self, user_id: int, query: CreateActionLogQuery, db: Session) -> None:
        group_ids = self.env.db.get_all_group_ids_for_user(user_id, db)

        # TODO: this is async, but check how long time this would take for like 5-10k groups
        for group_id in group_ids:
            self.leave_group(group_id, user_id, query, db)
