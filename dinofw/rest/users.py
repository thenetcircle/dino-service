import logging
from typing import List, Tuple

from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import GroupUpdatesQuery
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserStats
from dinofw.utils.config import GroupTypes

logger = logging.getLogger(__name__)


class UserResource(BaseResource):
    async def get_groups_for_user(
        self, user_id: int, query: GroupQuery, db: Session
    ) -> List[UserGroup]:
        user_groups: List[UserGroupBase] = self.env.db.get_groups_for_user(user_id, query, db)
        return self._to_user_group(user_groups)

    async def get_groups_updated_since(
        self, user_id: int, query: GroupUpdatesQuery, db: Session
    ) -> List[UserGroup]:
        user_groups: List[UserGroupBase] = self.env.db.get_groups_updated_since(user_id, query, db)
        return self._to_user_group(user_groups)

    async def get_user_stats(self, user_id: int, db: Session) -> UserStats:
        # if the user has more than 100 groups with unread messages in
        # it won't matter if the count is exact or not, just forget about
        # the super old ones (if a user reads a group, another unread
        # group will be selected next time for this query anyway)
        query = GroupQuery(
            per_page=100,
            only_unread=True,
            count_unread=True
        )

        user_groups: List[UserGroupBase] = self.env.db.get_groups_for_user(
            user_id, query, db, count_receiver_unread=False,
        )

        group_amounts = self.env.db.count_group_types_for_user(user_id, query, db)
        group_amounts = dict(group_amounts)

        unread_amount = 0

        last_read_group_id, last_read_time = self.env.db.get_last_read_for_user(user_id, db)
        last_sent_group_id, last_sent_time = self.env.db.get_last_sent_for_user(user_id, db)

        if last_sent_time is None:
            last_sent_time = self.long_ago
        if last_read_time is None:
            last_read_time = self.long_ago

        for user_group in user_groups:
            unread_amount += user_group.unread

        return UserStats(
            user_id=user_id,
            unread_amount=unread_amount,
            group_amount=group_amounts.get(GroupTypes.GROUP, 0),
            one_to_one_amount=group_amounts.get(GroupTypes.ONE_TO_ONE, 0),
            last_read_time=GroupQuery.to_ts(last_read_time),
            last_read_group_id=last_read_group_id,
            last_sent_time=GroupQuery.to_ts(last_sent_time),
            last_sent_group_id=last_sent_group_id,
        )
