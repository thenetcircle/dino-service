import logging
import random
from datetime import datetime
from typing import List
from uuid import uuid4 as uuid

import pytz

from dinofw.rest.base import BaseResource
from dinofw.rest.models import ActionLog, PaginationQuery, GroupUsers
from dinofw.rest.models import Group
from dinofw.rest.models import Histories
from dinofw.rest.models import HistoryQuery
from dinofw.rest.models import Message
from dinofw.rest.models import SearchQuery
from dinofw.rest.models import UserGroupStats

logger = logging.getLogger(__name__)


class GroupResource(BaseResource):
    async def users(self, group_id: str, query: PaginationQuery) -> GroupUsers:
        return GroupUsers(
            owner_id=1,
            users=[1, 2, 3, 4]
        )

    async def histories(self, group_id: str, user_id: int, query: HistoryQuery) -> List[Histories]:
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
            admin_id=0
        )

        histories = [
            Histories(message=self._message(group_id)),
            Histories(action_log=action_log)
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
            hide_before=0
        )

    async def search(self, query: SearchQuery) -> List[Group]:
        return [self._group()]

    async def get_group(self, group_id: str):
        return self._group(group_id)
