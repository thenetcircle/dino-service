import logging
from typing import List

from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import GroupQuery, GroupUpdatesQuery
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserStats

logger = logging.getLogger(__name__)


class UserResource(BaseResource):
    async def get_groups_for_user(
        self, user_id: int, query: GroupQuery, db: Session
    ) -> List[UserGroup]:
        count_unread = query.count_unread or False

        user_groups: List[UserGroupBase] = self.env.db.get_groups_for_user(
            user_id, query, db, count_unread=count_unread,
        )
        groups: List[UserGroup] = list()

        for user_group in user_groups:
            groups.append(
                BaseResource.group_base_to_user_group(
                    group_base=user_group.group,
                    stats_base=user_group.user_stats,
                    unread_count=user_group.unread_count,
                    user_count=user_group.user_count,
                    users=user_group.user_join_times,
                )
            )

        return groups

    async def get_groups_updated_since(
        self, user_id: int, query: GroupUpdatesQuery, db: Session
    ) -> List[UserGroup]:
        pass  # TODO: implement

    async def get_user_stats(self, user_id: int, db: Session) -> UserStats:
        # ordered by last_message_time, so we're likely to get all groups
        # with messages in them even if the user has more than 1k groups
        query = GroupQuery(per_page=1_000)

        user_groups: List[UserGroupBase] = self.env.db.get_groups_for_user(
            user_id, query, db, count_unread=False
        )

        unread_amount = 0
        owner_amount = 0
        max_last_read = self.long_ago
        max_last_sent = self.long_ago
        last_read_group_id = None
        last_sent_group_id = None

        for user_group in user_groups:
            group = user_group.group
            stats = user_group.user_stats

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
            group_amount=len(user_groups),
            owned_group_amount=owner_amount,
            last_read_time=GroupQuery.to_ts(max_last_read),
            last_read_group_id=last_read_group_id,
            last_send_time=GroupQuery.to_ts(max_last_sent),
            last_send_group_id=last_sent_group_id,
        )
