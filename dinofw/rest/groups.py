import itertools
from datetime import datetime as dt
from typing import List
from typing import Optional

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
from dinofw.rest.queries import CreateActionLogQuery, DeleteAttachmentQuery, AdminQuery, CountMessageQuery, \
    PublicGroupQuery, ExportQuery, PaginationQuery
from dinofw.rest.queries import CreateGroupQuery
from dinofw.rest.queries import GroupInfoQuery
from dinofw.rest.queries import JoinGroupQuery
from dinofw.rest.queries import MessageQuery
from dinofw.rest.queries import UpdateGroupQuery
from dinofw.rest.queries import UpdateUserGroupStats
from dinofw.utils import to_dt, is_non_zero, one_year_ago
from dinofw.utils import to_ts
from dinofw.utils import utcnow_dt
from dinofw.utils import utcnow_ts
from dinofw.utils.config import GroupTypes, GroupStatus
from dinofw.utils.convert import group_base_to_group
from dinofw.utils.convert import message_base_to_message
from dinofw.utils.convert import to_user_group_stats
from dinofw.utils.exceptions import InvalidRangeException, NoSuchGroupException, GroupIsFrozenOrArchivedException
from dinofw.utils.exceptions import UserIsKickedException


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

    async def get_all_public_groups(self, query: PublicGroupQuery, db: Session) -> List[Group]:
        group_bases = self.env.db.get_public_groups(query, db)
        groups = list()

        # TODO: skip users if only checking rooms for user ids
        for group in group_bases:
            _, first_users, n_users = self.env.db.get_users_in_group(group.group_id, db, include_group=False)
            groups.append(
                group_base_to_group(
                    group,
                    users=first_users,
                    user_count=n_users
                )
            )

        return groups

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

        return group_base_to_group(
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
            message_base_to_message(attachment)
            for attachment in attachments
        ]

    def mark_all_as_read(self, user_id: int, db: Session) -> None:
        group_ids_updated = self.env.db.mark_all_groups_as_read(user_id, db)

        group_to_user = self.env.db.get_user_ids_in_groups(group_ids_updated, db)
        now_dt = utcnow_dt()

        for group_id, user_ids in group_to_user.items():
            if user_id in user_ids:
                user_ids.remove(user_id)

            # marking a group as read sets bookmark=False
            self.env.client_publisher.read(
                group_id, user_id, user_ids, now_dt, bookmark=False
            )

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

        delete_before = self.env.db.get_delete_before(group_id, user_id_a, db)
        attachment_amount = await self.count_attachments_in_group_for_user(group_id, user_id_a, delete_before)

        # only need ths count for the calling user
        for user_stat in user_stats:
            if user_stat.user_id == user_id_a:
                user_stat.attachment_amount = attachment_amount

                # bookmarked groups counts as 1 unread message only if they
                # don't already have unread messages
                if user_stat.unread == 0 and user_stat.bookmark:
                    user_stat.unread = 1

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
            group=group_base_to_group(
                group=group,
                users=users_and_join_time,
                user_count=len(users_and_join_time),
                message_amount=message_amount
            ),
        )

    def set_last_updated_at_on_all_stats_related_to_user(self, user_id: int, db: Session) -> None:
        self.env.db.set_last_updated_at_on_all_stats_related_to_user(user_id, db)

    async def count_attachments_in_group_for_user(self, group_id: str, user_id: int, since: dt, query: CountMessageQuery = None) -> int:
        include_deleted = query is not None and query.include_deleted and is_non_zero(query.admin_id)

        if include_deleted:
            since = one_year_ago(since)
        else:
            the_count = self.env.cache.get_attachment_count_in_group_for_user(group_id, user_id)
            if the_count is not None:
                return the_count

        if query and query.only_sender:
            the_count = self.env.storage.count_attachments_in_group_since(group_id, since, sender_id=user_id)
        else:
            the_count = self.env.storage.count_attachments_in_group_since(group_id, since)

        if not include_deleted:
            # don't cache the value if we're including deleted messages
            self.env.cache.set_attachment_count_in_group_for_user(group_id, user_id, the_count)

        return the_count

    async def all_history_in_group(self, group_id: str) -> Histories:
        return Histories(
            messages=[
                message_base_to_message(message)
                for message in self.env.storage.get_all_messages_in_group(group_id)
            ]
        )

    def _check_that_query_is_valid_and_group_is_active(self, group_id: str, query: PaginationQuery, db: Session):
        # TODO: don't return history for archived or deleted groups unless admin

        if query.since is None and query.until is None:
            raise InvalidRangeException("both 'since' and 'until' was empty, need at least one")

        group_status = self.env.db.get_group_status(group_id, db)
        if group_status is None:
            raise NoSuchGroupException(group_id)

        if group_status != GroupStatus.DEFAULT:
            error_message = f"group {group_id} is {GroupStatus.to_str(group_status)}"
            raise GroupIsFrozenOrArchivedException(error_message)

    def _get_stats_and_check_that_user_is_not_kicked(self, group_id: str, user_id: int, db: Session):
        user_stats = self.env.db.get_user_stats_in_group(group_id, user_id, db)
        if user_stats.kicked:
            raise UserIsKickedException(group_id, user_id)

        return user_stats

    async def export_history_in_group(self, group_id: str, query: ExportQuery, db: Session) -> Histories:
        def get_messages():
            # need to batch query cassandra, can't filter by user id
            if query.user_id is not None:
                _messages = self.env.storage.get_messages_in_group_only_from_user(
                    group_id, user_stats, query
                )
            else:
                _messages = self.env.storage.export_history_in_group(
                    group_id, query
                )

            return [
                message_base_to_message(message)
                for message in _messages
            ]

        self._check_that_query_is_valid_and_group_is_active(group_id, query, db)
        if query.user_id is not None:
            user_stats = self._get_stats_and_check_that_user_is_not_kicked(group_id, query.user_id, db)

        return Histories(
            messages=get_messages()
        )

    async def histories(
        self, group_id: str, user_id: int, query: MessageQuery, db: Session
    ) -> Histories:
        def get_messages():
            # need to batch query cassandra, can't filter by user id
            if query.only_sender:
                _messages = self.env.storage.get_messages_in_group_only_from_user(
                    group_id, user_stats, query
                )
            else:
                _messages = self.env.storage.get_messages_in_group_for_user(
                    group_id, user_stats, query
                )

            return [
                message_base_to_message(message)
                for message in _messages
            ]

        self._check_that_query_is_valid_and_group_is_active(group_id, query, db)
        user_stats = self._get_stats_and_check_that_user_is_not_kicked(group_id, user_id, db)

        messages = get_messages()

        # history api can be called by the admin interface, in which case we don't want to change read status
        if query.admin_id is None or query.admin_id == 0:
            self._user_opens_conversation(group_id, user_id, user_stats, db)

        return Histories(
            messages=messages
        )

    async def count_messages_in_group(self, group_id: str) -> int:
        n_messages, until = self.env.cache.get_messages_in_group(group_id)

        if until is None:
            until = self.long_ago
            n_messages = 0
        else:
            until = to_dt(until)

        messages_since = self.env.storage.count_messages_in_group_since(group_id, until)
        total_messages = n_messages + messages_since
        now = utcnow_ts()

        self.env.cache.set_messages_in_group(group_id, total_messages, now)
        return total_messages

    async def get_all_user_group_stats(self, group_id: str, db: Session) -> List[UserGroupStats]:
        user_stats: List[UserGroupStatsBase] = self.env.db.get_all_user_stats_in_group(
            group_id, db
        )

        return [
            to_user_group_stats(user_stat)
            for user_stat in user_stats
        ]

    async def get_user_group_stats(
        self, group_id: str, user_id: int, db: Session
    ) -> Optional[UserGroupStats]:
        user_stats: UserGroupStatsBase = self.env.db.get_user_stats_in_group(
            group_id, user_id, db
        )

        if user_stats is None:
            return None

        return to_user_group_stats(user_stats)

    async def update_user_group_stats(
        self, group_id: str, user_id: int, query: UpdateUserGroupStats, db: Session
    ) -> None:
        self.env.db.update_user_group_stats(group_id, user_id, query, db)
        self.create_action_log(query.action_log, db, user_id=user_id, group_id=group_id)

    async def create_new_group(
        self, user_id: int, query: CreateGroupQuery, db: Session
    ) -> Group:
        now = utcnow_dt()
        now_ts = to_ts(now)

        group_base = self.env.db.create_group(user_id, query, now, db)
        users = {user_id: float(now_ts)}

        if query.users is not None and query.users:
            users.update({user_id: float(now_ts) for user_id in query.users})

        self.env.db.update_user_stats_on_join_or_create_group(
            group_id=group_base.group_id,
            users=users,
            now=now,
            group_type=group_base.group_type,
            db=db
        )

        group = group_base_to_group(
            group=group_base, users=users, user_count=len(users),
        )

        # notify users they're in a new group
        # self.env.client_publisher.group_change(group_base, list(users.keys()))

        return group

    async def update_group_information(
        self, group_id: str, query: UpdateGroupQuery, db: Session
    ) -> Message:
        self.env.db.update_group_information(group_id, query, db)
        self.env.db.set_last_updated_at_for_all_in_group(group_id, db)

        action_log = self.create_action_log(query.action_log, db, group_id=group_id)

        """
        user_ids_and_join_times = self.env.db.get_user_ids_and_join_time_in_group(
            group.group_id, db
        )
        user_ids = user_ids_and_join_times.keys()
        self.env.client_publisher.group_change(group, user_ids)
        """

        return action_log

    async def join_group(self, group_id: str, query: JoinGroupQuery, db: Session) -> Optional[Message]:
        now = utcnow_dt()
        now_ts = to_ts(now)

        user_ids_and_last_read = {
            user_id: float(now_ts)
            for user_id in query.users
        }

        group_type = self.env.cache.get_group_type(group_id)

        if group_type is None:
            group_types = self.env.db.get_group_types([group_id], db)
            if group_id not in group_types:
                raise NoSuchGroupException(group_id)

            group_type = group_types[group_id]
            self.env.cache.set_group_type(group_id, group_type)

        self.env.db.set_groups_updated_at([group_id], now, db)
        self.env.db.update_user_stats_on_join_or_create_group(
            group_id=group_id,
            users=user_ids_and_last_read,
            now=now,
            group_type=group_type,
            db=db
        )

        if not query.action_log:
            return None

        return self.create_action_log(query.action_log, db, group_id=group_id)

    def leave_groups(
            self, group_ids: List[str], user_id: int, query: CreateActionLogQuery, db: Session
    ) -> List[Message]:
        now = utcnow_dt()

        group_id_to_type = self.env.db.get_group_types(group_ids, db)

        self.env.db.copy_to_deleted_groups_table(group_id_to_type, user_id, db)
        self.env.db.remove_user_group_stats_for_user(group_ids, user_id, db)
        self.env.db.set_groups_updated_at(group_ids, now, db)
        self.env.cache.reset_count_group_types_for_user(user_id)

        action_logs = list()

        for group_id, group_type in group_id_to_type.items():
            # no need for an action log in 1v1 groups, it's not going to be shown anyway
            if group_type != GroupTypes.ONE_TO_ONE:
                action_logs.append(
                    self.create_action_log(query.action_log, db, user_id=user_id, group_id=group_id)
                )

        return action_logs

    def delete_attachments_in_group_for_user(
            self,
            group_id: str,
            user_id: int,
            query: DeleteAttachmentQuery,
            db: Session
    ) -> Message:
        group = self.env.db.get_group_from_id(group_id, db)

        attachments = self.env.storage.delete_attachments(
            group_id, group.created_at, user_id, query
        )

        now = utcnow_ts()
        user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db).keys()

        self.env.server_publisher.delete_attachments(group_id, attachments, user_ids, now)
        return self.create_action_log(query.action_log, db, user_id=user_id, group_id=group_id)

    def delete_all_groups_for_user(self, user_id: int, query: CreateActionLogQuery, db: Session) -> None:
        group_id_to_type = self.env.db.get_all_group_ids_and_types_for_user(user_id, db)

        # TODO: this is async, but check how long time this would take for like 5-10k groups
        self.leave_groups(group_id_to_type, user_id, query, db)
