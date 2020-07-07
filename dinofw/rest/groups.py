import logging
import random
from datetime import datetime
from typing import List
from uuid import uuid4 as uuid

import pytz
from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import GroupBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import (
    ActionLog,
    PaginationQuery,
    GroupUsers,
    GroupJoinQuery,
    Joiner,
    JoinerUpdateQuery,
    UpdateGroupQuery,
    CreateGroupQuery,
    AdminUpdateGroupQuery,
)
from dinofw.rest.models import Group
from dinofw.rest.models import Histories
from dinofw.rest.models import HistoryQuery
from dinofw.rest.models import Message
from dinofw.rest.models import SearchQuery
from dinofw.rest.models import UserGroupStats

logger = logging.getLogger(__name__)


class GroupResource(BaseResource):
    def __init__(self, env):
        self.env = env

    async def get_users_in_group(
        self, group_id: str, query: PaginationQuery
    ) -> GroupUsers:
        return GroupUsers(group_id=group_id, owner_id=1, users=[1, 2, 3, 4])

    async def histories(
        self, group_id: str, user_id: int, query: HistoryQuery
    ) -> List[Histories]:
        now = datetime.utcnow()
        now = now.replace(tzinfo=pytz.UTC)
        now = int(float(now.strftime("%s")))

        action_log = ActionLog(
            action_id=str(uuid()),
            user_id=int(random.random() * 1000000),
            group_id=group_id,
            message_id=str(uuid()),
            action_type=0,
            created_at=now,
            admin_id=0,
        )

        histories = [
            Histories(message=self._message(group_id)),
            Histories(action_log=action_log),
        ]

        return histories

    async def message(self, group_id: str, user_id: int, message_id: str) -> Message:
        return self._message(group_id, user_id, message_id)

    async def stats(self, group_id: str, user_id) -> UserGroupStats:
        amount = int(random.random() * 10000)
        now = datetime.utcnow()
        now = now.replace(tzinfo=pytz.UTC)
        now = int(float(now.strftime("%s")))

        return UserGroupStats(
            user_id=user_id,
            group_id=group_id,
            message_amount=amount,
            unread_amount=amount - int(random.random() * amount),
            last_read_time=now,
            last_send_time=now,
            hide_before=0,
        )

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

    async def joins(
        self, user_id: int, group_id: str, query: GroupJoinQuery
    ) -> List[Joiner]:
        return [self._join(group_id)]

    async def get_join_details(
        self, user_id: int, group_id: str, joiner_id: int
    ) -> Joiner:
        return self._join(group_id)

    async def delete_join_request(
        self, user_id: int, group_id: str, joiner_id: int
    ) -> None:
        pass

    async def admin_update_group_information(
        self, group_id, query: AdminUpdateGroupQuery
    ) -> Group:
        pass

    async def update_group_information(
        self, user_id: int, group_id: str, query: UpdateGroupQuery
    ) -> Group:
        pass

    async def update_join_request(
        self, user_id: int, group_id: str, joiner_id: int, query: JoinerUpdateQuery
    ) -> Joiner:
        return self._join(group_id, status=query.status)

    async def search(self, query: SearchQuery) -> List[Group]:
        return [self._group()]

    async def get_group(self, group_id: str):
        return self._group(group_id)

    async def hide_histories_for_user(
        self, group_id: str, user_id: int, query: HistoryQuery
    ):
        pass

    async def delete_one_group_for_user(self, user_id: int, group_id: str) -> None:
        pass

    async def delete_all_groups_for_user(self, user_id: int, group_id: str) -> None:
        pass

    @staticmethod
    def group_base_to_group(group: GroupBase, users: List[int], last_read: datetime) -> Group:
        group_dict = group.dict()

        group_dict["updated_at"] = CreateGroupQuery.to_ts(group_dict["updated_at"])
        group_dict["created_at"] = CreateGroupQuery.to_ts(group_dict["created_at"])
        group_dict["last_message_time"] = CreateGroupQuery.to_ts(group_dict["last_message_time"])
        group_dict["last_read"] = CreateGroupQuery.to_ts(last_read)
        group_dict["users"] = users

        return Group(**group_dict)
