import logging
from typing import List

from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import GroupQuery, GroupUpdatesQuery
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

    def _to_user_group(self, user_groups: List[UserGroupBase]):
        groups: List[UserGroup] = list()

        for user_group in user_groups:
            groups.append(
                BaseResource.group_base_to_user_group(
                    group_base=user_group.group,
                    stats_base=user_group.user_stats,
                    unread=user_group.unread,
                    receiver_unread=user_group.receiver_unread,
                    user_count=user_group.user_count,
                    users=user_group.user_join_times,
                )
            )

        return groups

    async def get_user_stats(self, user_id: int, db: Session) -> UserStats:
        # ordered by last_message_time, so we're likely to get all groups
        # with messages in them even if the user has more than 1k groups
        query = GroupQuery(per_page=1_000)

        user_groups: List[UserGroupBase] = self.env.db.get_groups_for_user(user_id, query, db)

        unread_amount = 0
        owner_amount = 0
        max_last_read = self.long_ago
        max_last_sent = self.long_ago
        last_read_group_id = None
        last_sent_group_id = None

        group_amounts = dict()

        for user_group in user_groups:
            group = user_group.group
            stats = user_group.user_stats

            if group.group_type not in group_amounts:
                group_amounts[group.group_type] = 0

            group_amounts[group.group_type] += 1

            last_message = group.last_message_time
            last_read = stats.last_read
            last_sent = stats.last_sent
            delete_before = stats.delete_before

            if last_message > last_read and last_message > delete_before:
                unread_amount += self.env.storage.get_unread_in_group(
                    group.group_id, user_id, last_read
                )

            if group.owner_id == user_id:
                owner_amount += 1

            if last_read is not None and last_read > max_last_read:
                max_last_read = last_read
                last_read_group_id = group.group_id

            if last_sent is not None and last_sent > max_last_sent:
                max_last_sent = last_sent
                last_sent_group_id = group.group_id

        return UserStats(
            user_id=user_id,
            unread_amount=unread_amount,
            group_amount=group_amounts.get(GroupTypes.GROUP, 0),
            one_to_one_amount=group_amounts.get(GroupTypes.ONE_TO_ONE, 0),
            owned_group_amount=owner_amount,
            last_read_time=GroupQuery.to_ts(max_last_read),
            last_read_group_id=last_read_group_id,
            last_send_time=GroupQuery.to_ts(max_last_sent),
            last_send_group_id=last_sent_group_id,
        )
