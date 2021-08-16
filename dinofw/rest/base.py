from abc import ABC
from datetime import datetime as dt
from typing import Dict, Union
from typing import List

import arrow
from loguru import logger
from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.endpoint import EventTypes
from dinofw.rest.models import Group
from dinofw.rest.models import GroupJoinTime
from dinofw.rest.models import GroupLastRead
from dinofw.rest.models import Message
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserGroupStats
from dinofw.rest.queries import AbstractQuery
from dinofw.rest.queries import ActionLogQuery
from dinofw.utils import utcnow_dt
from dinofw.utils import utcnow_ts
from dinofw.utils.decorators import time_method
from dinofw.utils.exceptions import NoSuchGroupException


class BaseResource(ABC):
    def __init__(self, env):
        self.env = env

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = arrow.Arrow.utcfromtimestamp(beginning_of_1995).datetime

    @time_method(logger, "_user_opens_conversation()")
    def _user_opens_conversation(self, group_id: str, user_id: int, user_stats: UserGroupStatsBase, db):
        """
        update database and cache with everything related to opening a conversation (if needed)
        """
        last_message_time = self.env.db.get_last_message_time_in_group(group_id, db)

        # if a user opens a conversation a second time and nothing has changed, we don't need to update
        if BaseResource.need_to_update_stats_in_group(user_stats, last_message_time):
            now_ts = utcnow_ts()
            now_dt = utcnow_dt(now_ts)

            # something changed, so update and set last_updated_time to sync to apps
            self.env.db.update_last_read_and_highlight_in_group_for_user(
                group_id, user_id, now_dt, db
            )

            # no point updating if already newer than last message (also skips
            # broadcasting unnecessary read-receipts)
            if last_message_time > user_stats.last_read:
                user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db)

                del user_ids[user_id]
                self.env.client_publisher.read(group_id, user_id, user_ids, now_ts)
                self.env.cache.set_unread_in_group(group_id, user_id, 0)

    def create_action_log(
        self,
            query: ActionLogQuery,
            db: Session,
            user_id: int = None,
            group_id: str = None,
    ) -> Message:
        # creating an action log is optional for the caller
        if query is None:
            return None  # noqa

        if query.user_id is not None:
            user_id = query.user_id

        if user_id is None and group_id is None:
            raise ValueError("either receiver_id or group_id is required in CreateActionLogQuery")

        if user_id is None:
            raise ValueError("no user_id in api path(?) and no user_id on ActionLogQuery; need one of them")

        if query.group_id is not None and len(query.group_id.strip()):
            group_id = query.group_id
        elif query.receiver_id is not None and query.receiver_id > 0:
            group_id = self._get_or_create_group_for_1v1(user_id, query.receiver_id, db)

        log = self.env.storage.create_action_log(user_id, group_id, query)
        self._user_sends_a_message(
            group_id,
            user_id=user_id,
            message=log,
            db=db,
            should_increase_unread=query.update_unread_count,
            update_last_message=query.update_last_message,
            event_type=EventTypes.ACTION_LOG
        )

        return BaseResource.message_base_to_message(log)

    def _user_sends_a_message(
            self,
            group_id: str,
            user_id: int,
            message: Union[MessageBase, Message],  # can either be a message or an action log
            db,
            should_increase_unread: bool,
            event_type: EventTypes,
            update_last_message: bool = True
    ):
        """
        update database and cache with everything related to sending a message
        """
        # cassandra DT is different from python DT
        now = utcnow_dt()
        user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db)

        self.env.db.update_group_new_message(
            message,
            now,
            db,
            sender_user_id=user_id,
            user_ids=user_ids.copy(),
            update_unread_count=should_increase_unread,
            update_last_message=update_last_message
        )

        if user_id not in user_ids:
            # if the user deleted the group, this is an action log for the
            # deletion, and we only have to un-hide it for the other user(s)
            self.env.cache.set_hide_group(group_id, False)
        else:
            # otherwise we update as normal
            self.env.db.update_last_read_and_sent_in_group_for_user(
                group_id, user_id, now, db
            )

        if event_type == EventTypes.MESSAGE:
            self.env.client_publisher.message(message, user_ids)

        elif event_type == EventTypes.ACTION_LOG:
            self.env.client_publisher.action_log(message, user_ids)

        elif event_type == EventTypes.EDIT:
            self.env.client_publisher.edit(message, user_ids)

        elif event_type == EventTypes.ATTACHMENT:
            self.env.client_publisher.attachment(message, user_ids)

    def _get_or_create_group_for_1v1(
        self, user_id: int, receiver_id: int, db: Session
    ) -> str:
        if user_id is None or receiver_id is None:
            raise ValueError(
                f"either receiver_id ({receiver_id}) or user_id ({user_id}) is None for get/create 1v1 group"
            )

        try:
            return self.env.db.get_group_id_for_1to1(user_id, receiver_id, db)
        except NoSuchGroupException:
            group = self.env.db.create_group_for_1to1(user_id, receiver_id, db)
            return group.group_id

    @staticmethod
    def need_to_update_stats_in_group(user_stats: UserGroupStatsBase, last_message_time: dt):
        if user_stats.bookmark:
            return True

        if user_stats.highlight_time > last_message_time:
            return True

        return last_message_time > user_stats.last_read

    @staticmethod
    @time_method(logger, "to_user_group()")
    def to_user_group(user_groups: List[UserGroupBase]):
        groups: List[UserGroup] = list()

        for user_group in user_groups:
            groups.append(
                BaseResource.group_base_to_user_group(
                    group_base=user_group.group,
                    stats_base=user_group.user_stats,
                    receiver_stats_base=user_group.receiver_user_stats,
                    unread=user_group.unread,
                    receiver_unread=user_group.receiver_unread,
                    user_count=user_group.user_count,
                    users=user_group.user_join_times,
                )
            )

        return groups

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
        user_count: int,
        message_amount: int = -1
    ) -> Group:
        group_dict = group.dict()

        users = [
            GroupJoinTime(user_id=user_id, join_time=join_time,)
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
        group_dict["first_message_time"] = AbstractQuery.to_ts(
            group_dict["first_message_time"]
        )
        group_dict["users"] = users
        group_dict["user_count"] = user_count
        group_dict["message_amount"] = message_amount

        return Group(**group_dict)

    @staticmethod
    def group_base_to_user_group(
        group_base: GroupBase,
        stats_base: UserGroupStatsBase,
        receiver_stats_base: UserGroupStatsBase,
        users: Dict[int, float],
        user_count: int,
        receiver_unread: int,
        unread: int,
    ) -> UserGroup:
        group = BaseResource.group_base_to_group(group_base, users, user_count)

        stats_dict = stats_base.__dict__
        stats_dict["unread"] = unread
        stats_dict["receiver_unread"] = receiver_unread
        stats_dict["receiver_highlight_time"] = AbstractQuery.to_ts(stats_base.receiver_highlight_time)

        if receiver_stats_base is not None:
            stats_dict["receiver_delete_before"] = AbstractQuery.to_ts(receiver_stats_base.delete_before)
            stats_dict["receiver_hide"] = receiver_stats_base.hide
            stats_dict["receiver_deleted"] = receiver_stats_base.deleted

        stats_dict["last_read_time"] = AbstractQuery.to_ts(stats_base.last_read)
        stats_dict["last_sent_time"] = AbstractQuery.to_ts(stats_base.last_sent)
        stats_dict["join_time"] = AbstractQuery.to_ts(stats_base.join_time)
        stats_dict["delete_before"] = AbstractQuery.to_ts(stats_base.delete_before)
        stats_dict["highlight_time"] = AbstractQuery.to_ts(
            stats_base.highlight_time, allow_none=True
        )
        stats_dict["first_sent"] = AbstractQuery.to_ts(
            stats_base.first_sent, allow_none=True
        )
        stats_dict["last_updated_time"] = AbstractQuery.to_ts(
            stats_base.last_updated_time
        )

        stats = UserGroupStats(**stats_dict)

        return UserGroup(group=group, stats=stats,)

    @staticmethod
    def user_group_stats_base_to_user_group_stats(user_stats: UserGroupStatsBase) -> UserGroupStats:
        stats_dict = user_stats.dict()

        stats_dict["last_read"] = AbstractQuery.to_ts(stats_dict["last_read"])
        stats_dict["last_sent"] = AbstractQuery.to_ts(stats_dict["last_sent"])
        stats_dict["delete_before"] = AbstractQuery.to_ts(stats_dict["delete_before"])

        return UserGroupStats(**stats_dict)

    @staticmethod
    def to_last_read(user_id: int, last_read: float) -> GroupLastRead:
        return GroupLastRead(user_id=user_id, last_read=last_read)
