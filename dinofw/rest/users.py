import logging
from datetime import datetime as dt
from time import time
from typing import List, Tuple

from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserGroupStats
from dinofw.rest.models import UserStats
from dinofw.rest.queries import ActionLogQuery
from dinofw.rest.queries import CreateActionLogQuery
from dinofw.rest.queries import GroupQuery
from dinofw.rest.queries import GroupUpdatesQuery
from dinofw.rest.queries import UserStatsQuery
from dinofw.utils import utcnow_ts
from dinofw.utils.config import GroupTypes

logger = logging.getLogger(__name__)


class UserResource(BaseResource):
    async def get_groups_for_user(
        self, user_id: int, query: GroupQuery, db: Session
    ) -> List[UserGroup]:
        user_groups: List[UserGroupBase] = self.env.db.get_groups_for_user(user_id, query, db, receiver_stats=True)
        return BaseResource.to_user_group(user_groups)

    def create_action_log_in_all_groups(
            self, user_id: int, query: ActionLogQuery, db: Session
    ) -> None:
        """
        This method is called only from an async rest api, so if it
        takes a while it doesn't matter for the caller.
        """
        group_ids_and_created_at: Tuple[str, dt] = self.env.db.get_group_ids_and_created_at_for_user(
            user_id, db
        )

        n_groups = len(group_ids_and_created_at)
        before = None
        if n_groups > 100:
            before = time()

        # ignore all fields except "payload"
        query.receiver_id = None
        query.user_id = None
        query.group_id = None

        for group_id, _ in group_ids_and_created_at:
            try:
                self.create_action_log(query, db, user_id=user_id, group_id=group_id)
            except Exception as e:
                self.logger.error(f"could not create action log in group {group_id} for user {user_id}: {str(e)}")
                self.logger.exception(e)

        if before is not None:
            elapsed = time() - before
            self.logger.info(f"creating action log in {n_groups} groups for user {user_id} took {elapsed:.1f}s")

    async def get_groups_updated_since(
        self, user_id: int, query: GroupUpdatesQuery, db: Session
    ) -> List[UserGroup]:
        user_groups: List[UserGroupBase] = self.env.db.get_groups_updated_since(user_id, query, db, receiver_stats=True)
        return BaseResource.to_user_group(user_groups)

    async def get_user_group_stats(self, group_id: str, user_id: int, db: Session) -> UserGroupStats:
        stats = self.env.db.get_user_stats_in_group(group_id, user_id, db)
        return BaseResource.user_group_stats_base_to_user_group_stats(stats)

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

        if query.count_unread:
            unread_amount = 0
            n_unread_groups = 0

            for user_group in user_groups:
                if user_group.unread == -1:
                    continue

                unread_amount += user_group.unread
                n_unread_groups += 1
        else:
            unread_amount = -1
            n_unread_groups = -1

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
            unread_groups_amount=n_unread_groups,
            group_amount=group_amounts.get(GroupTypes.GROUP, 0),
            one_to_one_amount=group_amounts.get(GroupTypes.ONE_TO_ONE, 0),
            last_sent_time=last_sent_time_ts,
            last_sent_group_id=last_sent_group_id,
        )

    def delete_all_user_attachments(self, user_id: int, query: CreateActionLogQuery, db: Session) -> None:
        group_created_at = self.env.db.get_group_ids_and_created_at_for_user(user_id, db)
        group_to_atts = self.env.storage.delete_attachments_in_all_groups(group_created_at, user_id)

        now = utcnow_ts()

        for group_id, attachments in group_to_atts.items():
            user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db).keys()
            self.env.server_publisher.delete_attachments(group_id, attachments, user_ids, now)
            self.create_action_log(query.action_log, db, user_id=user_id, group_id=group_id)
