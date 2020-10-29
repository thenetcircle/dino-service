import logging
from typing import List

import arrow
from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import GroupUpdatesQuery
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserStats
from dinofw.rest.models import UserStatsQuery
from dinofw.utils.config import GroupTypes

logger = logging.getLogger(__name__)


class UserResource(BaseResource):
    async def get_groups_for_user(
        self, user_id: int, query: GroupQuery, db: Session
    ) -> List[UserGroup]:
        user_groups: List[UserGroupBase] = self.env.db.get_groups_for_user(user_id, query, db, receiver_stats=True)
        return BaseResource.to_user_group(user_groups)

    async def get_groups_updated_since(
        self, user_id: int, query: GroupUpdatesQuery, db: Session
    ) -> List[UserGroup]:
        user_groups: List[UserGroupBase] = self.env.db.get_groups_updated_since(user_id, query, db, receiver_stats=True)
        return BaseResource.to_user_group(user_groups)

    async def get_user_stats(self, user_id: int, query: UserStatsQuery, db: Session) -> UserStats:
        # if the user has more than 100 groups with unread messages in
        # it won't matter if the count is exact or not, just forget about
        # the super old ones (if a user reads a group, another unread
        # group will be selected next time for this query anyway)
        sub_query = GroupQuery(
            per_page=100,
            only_unread=query.only_unread,
            count_unread=query.count_unread,
            hidden=query.hidden,
        )

        user_groups: List[UserGroupBase] = self.env.db.get_groups_for_user(
            user_id, sub_query, db, count_receiver_unread=False,
        )

        last_sent_time_ts = None
        last_sent_group_id = None
        group_amounts = {
            GroupTypes.GROUP: -1,
            GroupTypes.ONE_TO_ONE: -1,
        }

        unread_amount = 0
        for user_group in user_groups:
            unread_amount += user_group.unread

        # most calls to this api only needs to know the unread count and nothing else, and it's called OFTEN
        if not query.only_unread:
            group_amounts = self.env.db.count_group_types_for_user(user_id, sub_query, db)
            group_amounts = dict(group_amounts)

            last_sent_group_id, last_sent_time = self.env.db.get_last_sent_for_user(user_id, db)
            if last_sent_time is None:
                last_sent_time = self.long_ago

            last_sent_time_ts = GroupQuery.to_ts(last_sent_time)

        # TODO: what about last_update_time? it's in the model
        return UserStats(
            user_id=user_id,
            unread_amount=unread_amount,
            group_amount=group_amounts.get(GroupTypes.GROUP, 0),
            one_to_one_amount=group_amounts.get(GroupTypes.ONE_TO_ONE, 0),
            last_sent_time=last_sent_time_ts,
            last_sent_group_id=last_sent_group_id,
        )

    def delete_all_user_attachments(self, user_id: int, db: Session) -> None:
        group_created_at = self.env.db.get_group_ids_and_created_at_for_user(user_id, db)
        group_to_atts = self.env.storage.delete_attachments_in_all_groups(group_created_at, user_id)

        now = arrow.utcnow().float_timestamp

        for group_id, attachments in group_to_atts.items():
            user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db).keys()

            if not len(user_ids):
                continue

            for publisher in [self.env.client_publisher, self.env.server_publisher]:
                publisher.delete_attachments(group_id, attachments, user_ids, now)

            # TODO: how to tell apps an attachment was deleted?
            # self.env.db.update_group_updated_at ?
