from datetime import datetime
from datetime import timedelta
from typing import Tuple, Dict, Final

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dinofw.db.rdbms.models import GroupEntity
from dinofw.db.rdbms.models import UserGroupStatsEntity
from dinofw.rest.queries import UpdateUserGroupStats
from dinofw.utils import to_dt, group_id_to_users
from dinofw.utils import to_ts
from dinofw.utils import utcnow_dt
from dinofw.utils.config import GroupTypes
from dinofw.utils.exceptions import UserNotInGroupException, NoSuchGroupException

# don't recount unread messages in cassandra if last_read is within 5 seconds of last_message_time
NEAR_TIP_SLACK: Final[timedelta] = timedelta(seconds=5)


def _is_fast_last_read_only(q: UpdateUserGroupStats) -> bool:
    return (
        q is not None
        and q.last_read_time is not None
        and q.action_log is None
        and q.delete_before is None
        and q.highlight_time is None
        and q.highlight_limit is None
        and q.hide is None
        and (q.bookmark is None or q.bookmark is False)
        and q.bookmark is None
        and q.pin is None
        and q.rating is None
        and q.notifications is None
        and q.kicked is None
    )


class UpdateUserGroupStatsHandler:
    def __init__(self, env, handler):
        self.env = env
        self.handler = handler

    async def _clear_oldest_highlight_if_limit_reached(
            self,
            group_id: str,
            user_id: int,
            query: UpdateUserGroupStats,
            db: AsyncSession
    ) -> None:
        """
        Need to make up to two queries; one to get the group_ids of all
        highlighted groups, then another query to get both this user's
        stat and the receiving user's stat, so we can reset the highlight
        time of any and all groups outside the limit. The limit is an
        api parameter, and can change, so it's not always only gonna be
        1 group that exceeds the limit (e.g. paying user can have 10
        highlighted groups, then the user stops paying and they can now
        only have 5 highlighted groups, then 5 groups needs to have their
        highlight time reset).
        """
        limit = query.highlight_limit
        if limit is None:
            return

        highlighted_groups = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(UserGroupStatsEntity.user_id == user_id)

            # in case we set it on an already highlighted group; otherwise it will count +1 towards the limit
            .filter(UserGroupStatsEntity.group_id != group_id)

            .filter(UserGroupStatsEntity.highlight_time > self.handler.long_ago)
            .order_by(UserGroupStatsEntity.highlight_time)
            .all()
        )

        if len(highlighted_groups) < query.highlight_limit:
            return

        # ordered by highlight time; only leave limit-1 groups (-1 since we'll be adding a new one later)
        start_idx = len(highlighted_groups) - limit + 1
        oldest_groups = highlighted_groups[:start_idx]

        # need to get the receiver's stat entry as well
        stats_in_groups = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(UserGroupStatsEntity.group_id.in_(
                [stat.group_id for stat in oldest_groups]
            ))
            .all()
        )

        # reset the highlight time in the oldest groups
        for stat in stats_in_groups:
            stat.highlight_time = self.handler.long_ago
            stat.receiver_highlight_time = self.handler.long_ago
            db.add(stat)

    async def _set_highlight_time(
            self,
            group_id: str,
            user_id: int,
            user_stats: UserGroupStatsEntity,
            that_user_stats: UserGroupStatsEntity,
            highlight_time: datetime,
            query: UpdateUserGroupStats,
            db: AsyncSession
    ):
        user_stats.highlight_time = highlight_time

        # save the highlight time on the other user, to not have to
        # do a second query to fetch it when listing groups
        if that_user_stats is not None:
            that_user_stats.receiver_highlight_time = highlight_time

        await self._clear_oldest_highlight_if_limit_reached(group_id, user_id, query, db)

        # always becomes unhidden if highlighted
        user_stats.hide = False
        await self.env.cache.set_hide_group(group_id, False, [user_id])

    async def _set_delete_before(
            self,
            group_id: str,
            user_id: int,
            user_stats: UserGroupStatsEntity,
            delete_before: datetime,
            unread_count_before_changing: int,
            db: AsyncSession
    ) -> None:
        user_stats.delete_before = delete_before

        # otherwise a deleted group could have unread messages
        user_stats.last_read = delete_before

        # for syncing deletions to apps, returned in /updates api
        user_stats.deleted = True

        # deleted groups can't be bookmarked or hidden
        user_stats.bookmark = False
        user_stats.hide = False

        # set to 0 and not -1, since we know there's actually 0 sent
        # messages from this user after he deletes a conversation,
        # so there's no need to count from cassandra from now on
        user_stats.sent_message_count = 0
        await self.env.cache.set_sent_message_count_in_group_for_user(group_id, user_id, 0)

        # no pipeline for these two, might have to run multiple queries to correct negative value
        # self.env.cache.decrease_total_unread_message_count(user_id, unread_count_before_changing)
        await self.env.cache.reset_total_unread_message_count(user_id)
        await self.env.cache.remove_unread_group(user_id, group_id)

        async with self.env.cache.pipeline() as p:
            # need to reset unread count when deleting a group
            user_stats.unread_count = 0
            await self.env.cache.set_unread_in_group(group_id, user_id, 0, pipeline=p)

            # update the cached value of delete before, and also remove
            # the cached count of attachments in this group for this
            # user; it will be recounted and cached again the next time
            # it's requested
            await self.env.cache.set_delete_before(group_id, user_id, to_ts(delete_before), pipeline=p)
            await self.env.cache.remove_attachment_count_in_group_for_users(group_id, [user_id], pipeline=p)

    async def _set_hide(
            self,
            group_id: str,
            user_id: int,
            user_stats: UserGroupStatsEntity,
            unread_count_before_changing: int,
            query: UpdateUserGroupStats
    ) -> None:
        user_stats.hide = query.hide

        async with self.env.cache.pipeline() as p:
            await self.env.cache.set_hide_group(group_id, query.hide, [user_id], pipeline=p)

            # TODO: there's a somewhere that's using decrease_total_unread_message_count(),
            #  the cached count sometimes becomes the wrong value, so reset() instead for now
            await self.env.cache.reset_total_unread_message_count(user_id, pipeline=p)

            if query.hide:
                # no pipline for removing, might have to run multiple queries
                await self.env.cache.remove_unread_group(user_id, group_id, pipeline=p)

                """
                # bookmark AND hide makes it a bit tricky
                change_by = unread_count_before_changing
                if user_stats.bookmark:
                    change_by = 1
    
                self.env.cache.decrease_total_unread_message_count(user_id, change_by)
                """
            else:
                await self.env.cache.add_unread_group([user_id], group_id, pipeline=p)
                # self.env.cache.increase_total_unread_message_count(
                #     [user_id], unread_count_before_changing, pipeline=p
                # )

    async def _set_bookmark(
            self,
            group_id: str,
            user_id: int,
            user_stats: UserGroupStatsEntity,
            query: UpdateUserGroupStats
    ) -> None:
        user_stats.bookmark = query.bookmark

        if query.bookmark:
            async with self.env.cache.pipeline() as p:
                await self.env.cache.increase_total_unread_message_count([user_id], 1, pipeline=p)
                await self.env.cache.add_unread_group([user_id], group_id, pipeline=p)
        # doesn't work well with pipeline
        else:
            # bookmark always counts as 1 unread, so just decrease by 1
            # self.env.cache.decrease_total_unread_message_count(user_id, 1)
            await self.env.cache.reset_total_unread_message_count(user_id)
            await self.env.cache.remove_unread_group(user_id, group_id)

    async def _set_last_read(
            self,
            group_id: str,
            user_id: int,
            last_read: datetime,
            unread_count_before_changing: int,
            group: GroupEntity,
            user_stats: UserGroupStatsEntity,
            that_user_stats: UserGroupStatsEntity,
            db: AsyncSession
    ) -> None:
        # check previous last read before updating it, and send read-receipts to other users
        # if last_message_time is more than the previous last_read
        if group.last_message_time > user_stats.last_read:
            user_ids = await self.env.db.get_user_ids_and_join_time_in_group(group_id, db)

            if user_id in user_ids:
                del user_ids[user_id]

            self.env.client_publisher.read(
                group_id, user_id, list(user_ids.keys()), last_read, bookmark=user_stats.bookmark
            )

        # now check the new last read time, but only recount if there's potentially unread messages
        if group.last_message_time > last_read:
            user_stats.unread_count = await self.env.storage.get_unread_in_group(group_id, user_id, last_read)
            reset_unread_in_cache = False
        else:
            user_stats.unread_count = 0
            reset_unread_in_cache = True

        # when updating last read, we reset the mention count to 0 and bookmark to false
        user_stats.mentions = 0
        user_stats.bookmark = False
        user_stats.last_read = last_read

        await self.env.cache.last_read_was_updated(
            group_id, user_id, last_read, reset_unread_in_cache=reset_unread_in_cache
        )

        # highlight time is removed if a user reads a conversation
        user_stats.highlight_time = self.handler.long_ago
        if that_user_stats is not None:
            that_user_stats.receiver_highlight_time = self.handler.long_ago

    async def _fast_update_last_read(
            self,
            group_id: str,
            user_id: int,
            new_last_read: datetime,
            db: AsyncSession
    ) -> None:
        """
        Previously the p95 for the "update user stats in group" api was around 450ms when going through the full ORM
        path for all the update possibilities, but 99% of the time, the api is called only to update the
        last_read time. This method is the fast path for updating last_read only, which only loads the bare minimum
        from the DB and does a single UPDATE statement, plus all cache updates are done in a single pipeline.
        """
        async def _fetch_last_read_and_bookmark() -> Tuple[datetime, bool]:
            _res = await db.execute(
                select(
                    UserGroupStatsEntity.last_read,
                    UserGroupStatsEntity.bookmark
                ).where(
                    UserGroupStatsEntity.group_id == group_id,
                    UserGroupStatsEntity.user_id == user_id,
                )
            )
            _row = _res.one_or_none()
            if _row is None:
                raise UserNotInGroupException(
                    f"tried to update group stats for user {user_id} not in group {group_id}"
                )
            return _row

        async def _get_group_type_and_last_msg_time() -> Tuple[int, datetime]:
            _g = await db.execute(
                select(
                    GroupEntity.group_type,
                    GroupEntity.last_message_time
                )
                .where(GroupEntity.group_id == group_id)
            )
            _row = _g.one_or_none()
            if _row is None:
                raise NoSuchGroupException(group_id)
            return _row

        async def _send_read_receipts() -> None:
            user_ids_join_time: Dict[int, str] = \
                await self.env.db.get_user_ids_and_join_time_in_group(group_id, db)

            if user_id in user_ids_join_time:
                user_ids_join_time = dict(user_ids_join_time)  # shallow copy
                user_ids_join_time.pop(user_id, None)

            self.env.client_publisher.read(
                group_id, user_id, list(user_ids_join_time.keys()), new_last_read, bookmark=was_bookmarked
            )

        # 1) Fetch only what we absolutely need: previous last_read & bookmark
        prev_last_read, was_bookmarked = await _fetch_last_read_and_bookmark()

        # if we’re not advancing, do nothing
        if new_last_read is None or (prev_last_read is not None and new_last_read <= prev_last_read):
            return

        # 2) Fetch only last_message_time + group_type for the group
        group_type, group_last_msg_time = await _get_group_type_and_last_msg_time()

        # 3) Send read receipts only to 1v1 groups and only if the new read crosses the previous boundary
        if group_type == GroupTypes.ONE_TO_ONE and group_last_msg_time > prev_last_read:
            await _send_read_receipts()

        # 4) Only hit Cassandra if there *can* be unread after new_last_read (don't spend time counting in cassandra
        # if the resulting unread is only 1 or 2 messages anyway (common in "hot" active groups)
        if group_last_msg_time <= new_last_read + NEAR_TIP_SLACK:
            unread = 0
            reset_unread_in_cache = True
        else:
            unread = await self.env.storage.get_unread_in_group(group_id, user_id, new_last_read)
            reset_unread_in_cache = False

        # 5) Minimal DB updates (single UPDATE for this row; no ORM add/flush)
        await db.execute(
            update(UserGroupStatsEntity)
            .where(
                UserGroupStatsEntity.group_id == group_id,
                UserGroupStatsEntity.user_id == user_id,
            )
            .values(
                last_updated_time=utcnow_dt(),
                last_read=new_last_read,
                unread_count=unread,
                mentions=0,
                bookmark=False,  # bookmark always drops on read
                highlight_time=self.handler.long_ago,  # highlight cleared on read
            )
        )

        # Also drop receiver_highlight_time on the other row in 1:1 groups, without loading it
        if group_type == GroupTypes.ONE_TO_ONE:
            other_user_id = [uid for uid in group_id_to_users(group_id) if uid != user_id][0]
            await db.execute(
                update(UserGroupStatsEntity)
                .where(
                    UserGroupStatsEntity.group_id == group_id,
                    UserGroupStatsEntity.user_id == other_user_id,
                )
                .values(receiver_highlight_time=self.handler.long_ago)
            )

        # 6) Update all relevant redis keys in a pipeline
        await self.env.cache.last_read_was_updated(
            group_id, user_id, new_last_read, reset_unread_in_cache=reset_unread_in_cache
        )

        await db.commit()

    async def update(
            self,
            group_id: str,
            user_id: int,
            query: UpdateUserGroupStats,
            db: AsyncSession
    ) -> None:
        if _is_fast_last_read_only(query):
            await self._fast_update_last_read(group_id, user_id, to_dt(query.last_read_time), db)
            return

        user_stats, that_user_stats, group = await self.handler.get_both_user_stats_in_group(group_id, user_id, query, db)

        if user_stats is None:
            raise UserNotInGroupException(
                f"tried to update group stats for user {user_id} not in group {group_id}"
            )

        unread_count_before_changing = user_stats.unread_count
        last_read = to_dt(query.last_read_time, allow_none=True)
        delete_before = to_dt(query.delete_before, allow_none=True)
        highlight_time = to_dt(
            query.highlight_time, allow_none=True
        )
        now = utcnow_dt()

        # used by apps to sync changes
        user_stats.last_updated_time = now

        # handle kick first, might be updating bookmark/etc. after in the same request for some reason
        if query.kicked is not None:
            # only reset if this is the first kick
            if not user_stats.kicked and query.kicked:
                await self.env.cache.remove_user_id_and_join_time_in_groups_for_user([group_id], user_id)
                if user_stats.unread_count > 0:
                    # self.env.cache.decrease_total_unread_message_count(user_id, user_stats.unread_count)
                    await self.env.cache.reset_total_unread_message_count(user_id)

                user_stats.mentions = 0
                user_stats.unread_count = 0
                user_stats.bookmark = False
                user_stats.pin = False

            # set the new value, whether true or false
            user_stats.kicked = query.kicked

        if query.bookmark is not None:
            await self._set_bookmark(group_id, user_id, user_stats, query)

            # set the last read time to now(), since a user can't remove a
            # bookmark without opening the conversation
            if query.bookmark is False and query.last_read_time is None:
                last_read = now

        if query.pin is not None:
            user_stats.pin = query.pin

        if query.rating is not None:
            user_stats.rating = query.rating

        if query.notifications is not None:
            user_stats.notifications = query.notifications

            # force a recount of total unreads, since previously only mentions were cached for this group
            await self.env.cache.reset_total_unread_message_count(user_id)

        if last_read is not None:
            await self._set_last_read(
                group_id, user_id, last_read, unread_count_before_changing, group, user_stats, that_user_stats, db
            )

        if delete_before is not None:
            await self._set_delete_before(group_id, user_id, user_stats, delete_before, unread_count_before_changing, db)

        # can't set highlight time if also setting last read time
        if highlight_time is not None and last_read is None:
            await self._set_highlight_time(group_id, user_id, user_stats, that_user_stats, highlight_time, query, db)

        elif query.hide is not None:
            await self._set_hide(group_id, user_id, user_stats, unread_count_before_changing, query)

        if query.hide is not None or query.delete_before is not None:
            await self.env.cache.reset_count_group_types_for_user(user_id)

        db.add(user_stats)
        if that_user_stats is not None:
            db.add(that_user_stats)

        await db.commit()
