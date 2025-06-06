from datetime import datetime as dt
from time import time
from typing import List, Tuple, Set, Optional

from loguru import logger
from sqlalchemy.orm import Session

from dinofw.db.rdbms.schemas import UserGroupBase, DeletedStatsBase
from dinofw.rest.base import BaseResource
from dinofw.rest.models import UserGroup, LastReads, DeletedStats, UnDeletedGroup
from dinofw.rest.models import UserStats
from dinofw.rest.queries import ActionLogQuery, UserIdQuery, SessionUser
from dinofw.rest.queries import DeleteAttachmentQuery
from dinofw.rest.queries import GroupQuery
from dinofw.rest.queries import GroupUpdatesQuery
from dinofw.rest.queries import UserStatsQuery
from dinofw.utils import to_ts
from dinofw.utils import utcnow_ts
from dinofw.utils.config import GroupTypes
from dinofw.utils.convert import to_user_group, to_last_reads, to_deleted_stats, to_undeleted_stats


class UserResource(BaseResource):
    async def get_next_client_id(self, domain: str, user_id: int) -> str:
        return await self.env.cache.get_next_client_id(domain, user_id)

    async def get_deleted_groups(self, user_id: int, db: Session) -> List[DeletedStats]:
        deleted_groups: List[DeletedStatsBase] = await self.env.db.get_deleted_groups_for_user(user_id, db)
        return to_deleted_stats(deleted_groups)

    async def update_user_sessions(self, users: List[SessionUser], db: Session):
        current_online_users: Set[int] = await self.env.db.get_online_users(db)

        online_users = {user.user_id for user in users if user.is_online}
        offline_users = {
            user.user_id for user in users
            # avoid sending duplicate offline events to kafka by checking currently online users
            if not user.is_online and user.user_id in current_online_users
        }

        for user_id in current_online_users:
            if user_id not in online_users:
                offline_users.add(user_id)

        offline_users = list(offline_users)
        online_users = list(online_users)

        await self.env.cache.set_online_users(offline_users, online_users)

        # only notify if someone left
        if offline_users:
            self.env.server_publisher.offline_users(offline_users)

    async def update_real_time_user_session(self, user: SessionUser):
        logger.debug(f"update_real_time_user_session: {user.json()}")
        if user.is_online:
            await self.env.cache.set_online_user(user.user_id)
        else:
            await self.env.cache.set_offline_user(user.user_id)
            self.env.server_publisher.offline_users([user.user_id])

    async def get_all_user_stats_for_user(
        self, user_id: int, db: Session
    ) -> List[UnDeletedGroup]:
        groups: List[Tuple[str, int, dt]] = await self.env.db.get_group_id_type_join_time_for_user(
            user_id, db
        )

        return to_undeleted_stats(groups)

    async def get_groups_for_user(
        self, user_id: int, query: GroupQuery, db: Session
    ) -> List[UserGroup]:
        user_groups: List[UserGroupBase] = await self.env.db.get_groups_for_user(
            user_id, query, db
        )

        deleted_groups: Optional[List[DeletedStatsBase]] = None

        if query.include_deleted:
            deleted_groups: List[DeletedStatsBase] = await self.env.db.get_deleted_groups_for_user(user_id, db)

        return to_user_group(user_groups, deleted_groups=deleted_groups)

    async def create_action_log_in_all_groups(
            self, user_id: int, query: ActionLogQuery, db: Session
    ) -> None:
        """
        This method is called only from an async rest api, so if it
        takes a while it doesn't matter for the caller.
        """
        group_ids_and_created_at: Tuple[str, dt] = await self.env.db.get_group_ids_and_created_at_for_user(
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
                await self.create_action_log(query, db, user_id=user_id, group_id=group_id)
            except Exception as e:
                logger.error(f"could not create action log in group {group_id} for user {user_id}: {str(e)}")
                logger.exception(e)

        if before is not None:
            elapsed = time() - before
            logger.info(f"creating action log in {n_groups} groups for user {user_id} took {elapsed:.1f}s")

    async def get_groups_updated_since(
        self, user_id: int, query: GroupUpdatesQuery, db: Session
    ) -> List[UserGroup]:
        user_groups: List[UserGroupBase] = await self.env.db.get_groups_updated_since(
            user_id, query, db
        )

        return to_user_group(user_groups)

    async def get_public_groups_updated_since(
        self, user_id: int, query: GroupUpdatesQuery, db: Session
    ) -> List[UserGroup]:
        user_groups: List[UserGroupBase] = await self.env.db.get_groups_updated_since(
            user_id, query, db, public_only=True
        )

        return to_user_group(user_groups)

    async def get_last_read(self, group_id: str, query: UserIdQuery, db: Session) -> LastReads:
        if query.user_id is None:
            last_reads = await self.env.db.get_last_reads_in_group(group_id, db)
        else:
            last_reads = await self.env.db.get_last_read_for_user(group_id, query.user_id, db)

        return to_last_reads(group_id, last_reads)

    async def count_unread(self, user_id: int, db: Session) -> (int, int):
        # TODO: need to update cache on hide, bookmark, read, send, delete, highlight(?)
        #  * highlight(?)   :
        #  * delete         : done? not yet, updating delete_before needs a fix
        #  * hide           : done?
        #  * send           : done?
        #  * read           : done?
        #  * bookmark       : done?

        unread_count, n_unread_groups = await self.env.cache.get_total_unread_count(user_id)
        if unread_count is not None:
            return unread_count, n_unread_groups

        unread_count, unread_groups = await self.env.db.count_total_unread(user_id, db)
        n_unread_groups = len(unread_groups)

        await self.env.cache.set_total_unread_count(user_id, unread_count, unread_groups)
        return unread_count, n_unread_groups

    async def get_user_stats(self, user_id: int, query: UserStatsQuery, db: Session) -> UserStats:
        if query.count_unread:
            unread_amount, n_unread_groups = await self.count_unread(user_id, db)
        else:
            unread_amount = -1
            n_unread_groups = -1

        group_amounts = await self.env.db.count_group_types_for_user(
            user_id,
            GroupQuery(
                per_page=-1,
                only_unread=query.only_unread,
                count_unread=query.count_unread,
                hidden=query.hidden,
            ),
            db
        )
        group_amounts = dict(group_amounts)

        last_sent_group_id, last_sent_time = await self.env.db.get_last_sent_for_user(user_id, db)
        if last_sent_time is None:
            last_sent_time = self.long_ago

        last_sent_time_ts = to_ts(last_sent_time)

        # TODO: what about last_update_time? it's in the model
        return UserStats(
            user_id=user_id,
            unread_amount=unread_amount,
            unread_groups_amount=n_unread_groups,
            group_amount=group_amounts.get(GroupTypes.PRIVATE_GROUP, 0) + group_amounts.get(GroupTypes.PUBLIC_ROOM, 0),
            one_to_one_amount=group_amounts.get(GroupTypes.ONE_TO_ONE, 0),
            last_sent_time=last_sent_time_ts,
            last_sent_group_id=last_sent_group_id,
        )

    async def delete_all_user_attachments(self, user_id: int, query: DeleteAttachmentQuery, db: Session) -> None:
        group_created_at = await self.env.db.get_group_ids_and_created_at_for_user(user_id, db)
        group_to_atts = await self.env.storage.delete_attachments_in_all_groups(group_created_at, user_id, query)

        now = utcnow_ts()

        for group_id, attachments in group_to_atts.items():
            user_ids = (await self.env.db.get_user_ids_and_join_time_in_group(group_id, db)).keys()
            self.env.server_publisher.delete_attachments(group_id, attachments, user_ids, now)
            await self.create_action_log(query.action_log, db, user_id=user_id, group_id=group_id)
