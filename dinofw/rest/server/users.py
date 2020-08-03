import logging
from typing import List, Tuple, Any

from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import GroupBase, UserGroupStatsBase
from dinofw.rest.server.base import BaseResource
from dinofw.rest.server.models import Group, GroupJoinTime
from dinofw.rest.server.models import GroupQuery
from dinofw.rest.server.models import UserStats

logger = logging.getLogger(__name__)


class UserResource(BaseResource):
    async def get_groups_for_user(
        self, user_id: int, query: GroupQuery, db: Session
    ) -> List[Group]:
        """
        TODO: could give full list of of groups with unread messages in them:

        select
            g.group_id, g.last_message_time, u.last_read_time
        from groups g
            inner join user_group_stats u on g.group_id = u.group_id
        where
            user_id = 1234 and
            last_read < last_message_time;
        """

        groups_stats_and_users = self.env.db.get_groups_for_user(user_id, query, db)
        groups = list()

        for group, user_group_stats, users, user_count in groups_stats_and_users:
            group_dict = group.dict()

            user_joins = [
                GroupJoinTime(user_id=one_user_id, join_time=join_time)
                for one_user_id, join_time in users.items()
            ]

            group_dict["users"] = user_joins
            group_dict["user_count"] = user_count
            group_dict["last_read"] = GroupQuery.to_ts(user_group_stats.last_read)

            group_dict["created_at"] = GroupQuery.to_ts(group_dict["created_at"])
            group_dict["updated_at"] = GroupQuery.to_ts(group_dict["updated_at"])
            group_dict["last_message_time"] = GroupQuery.to_ts(
                group_dict["last_message_time"]
            )

            groups.append(Group(**group_dict))

        return groups

    async def get_user_stats(self, user_id: int, db: Session) -> UserStats:
        # ordered by last_message_time, so we're likely to get all groups
        # with messages in them even if the user has more than 1k groups
        query = GroupQuery(per_page=1_000)

        groups_stats_and_users: List[
            Tuple[GroupBase, UserGroupStatsBase, Any, Any]
        ] = self.env.db.get_groups_for_user(user_id, query, db, count_users=False)

        unread_amount = 0
        owner_amount = 0
        max_last_read = self.long_ago
        max_last_sent = self.long_ago
        last_read_group_id = None
        last_sent_group_id = None

        for group, stats, _, _ in groups_stats_and_users:
            last_message = group.last_message_time
            last_read = stats.last_read
            last_sent = stats.last_sent
            delete_before = stats.delete_before

            if last_message > last_read and last_message > delete_before:
                unread_amount += self.env.storage.get_unread_in_group(
                    group.group_id,
                    user_id,
                    stats.last_read
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
            group_amount=len(groups_stats_and_users),
            owned_group_amount=owner_amount,
            last_read_time=GroupQuery.to_ts(max_last_read),
            last_read_group_id=last_read_group_id,
            last_send_time=GroupQuery.to_ts(max_last_sent),
            last_send_group_id=last_sent_group_id,
        )
