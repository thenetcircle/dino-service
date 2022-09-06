from sqlalchemy.orm import Session
from datetime import datetime

from dinofw.db.rdbms.models import GroupEntity
from dinofw.db.rdbms.models import UserGroupStatsEntity
from dinofw.rest.queries import UpdateUserGroupStats
from dinofw.utils import to_dt
from dinofw.utils import to_ts
from dinofw.utils import utcnow_dt
from dinofw.utils.exceptions import UserNotInGroupException


class UpdateUserGroupStatsHandler:
    def __init__(self, env, handler):
        self.env = env
        self.handler = handler

    def _clear_oldest_highlight_if_limit_reached(
            self,
            group_id: str,
            user_id: int,
            query: UpdateUserGroupStats,
            db: Session
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

        highlighted_groups = (
            db.query(UserGroupStatsEntity)
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
        stats_in_groups = (
            db.query(UserGroupStatsEntity)
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

    def _set_highlight_time(
            self,
            group_id: str,
            user_id: int,
            user_stats: UserGroupStatsEntity,
            that_user_stats: UserGroupStatsEntity,
            highlight_time: datetime,
            query: UpdateUserGroupStats,
            db: Session
    ):
        user_stats.highlight_time = highlight_time

        # save the highlight time on the other user, to not have to
        # do a second query to fetch it when listing groups
        if that_user_stats is not None:
            that_user_stats.receiver_highlight_time = highlight_time

        self._clear_oldest_highlight_if_limit_reached(group_id, user_id, query, db)

        # always becomes unhidden if highlighted
        user_stats.hide = False
        self.env.cache.set_hide_group(group_id, False, [user_id])

    def _set_delete_before(
            self,
            group_id: str,
            user_id: int,
            user_stats: UserGroupStatsEntity,
            delete_before: datetime,
            unread_count_before_changing: int,
            db: Session
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
        self.env.db.set_sent_message_count(group_id, user_id, 0, db)

        # no pipeline for these two, might have to run multiple queries to correct negative value
        self.env.cache.decrease_total_unread_message_count(user_id, unread_count_before_changing)
        self.env.cache.remove_unread_group(user_id, group_id)

        with self.env.cache.pipeline() as p:
            # need to reset unread count when deleting a group
            user_stats.unread_count = 0
            self.env.cache.set_unread_in_group(group_id, user_id, 0, pipeline=p)

            # update the cached value of delete before, and also remove
            # the cached count of attachments in this group for this
            # user; it will be recounted and cached again the next time
            # it's requested
            self.env.cache.set_delete_before(group_id, user_id, to_ts(delete_before), pipeline=p)
            self.env.cache.remove_attachment_count_in_group_for_users(group_id, [user_id], pipeline=p)

    def _set_hide(
            self,
            group_id: str,
            user_id: int,
            user_stats: UserGroupStatsEntity,
            unread_count_before_changing: int,
            query: UpdateUserGroupStats
    ) -> None:
        user_stats.hide = query.hide
        self.env.cache.set_hide_group(group_id, query.hide, [user_id])

        if query.hide:
            # no pipline for removing, might have to run multiple queries
            self.env.cache.remove_unread_group(user_id, group_id)

            # bookmark AND hide makes it a bit tricky
            change_by = unread_count_before_changing
            if user_stats.bookmark:
                change_by = 1

            self.env.cache.decrease_total_unread_message_count(user_id, change_by)
        else:
            with self.env.cache.pipeline() as p:
                self.env.cache.add_unread_group([user_id], group_id, pipeline=p)
                self.env.cache.increase_total_unread_message_count(
                    [user_id], unread_count_before_changing, pipeline=p
                )

    def _set_last_read(
            self,
            group_id: str,
            user_id: int,
            last_read: datetime,
            unread_count_before_changing: int,
            group: GroupEntity,
            user_stats: UserGroupStatsEntity,
            that_user_stats: UserGroupStatsEntity,
            db: Session
    ) -> None:
        # check previous last read before updating it, and send read-receipts to other users
        # if last_message_time is more than the previous last_read
        if group.last_message_time > user_stats.last_read:
            user_ids = self.env.db.get_user_ids_and_join_time_in_group(group_id, db)

            del user_ids[user_id]
            self.env.client_publisher.read(
                group_id, user_id, list(user_ids.keys()), last_read, bookmark=user_stats.bookmark
            )

        # TODO: use pipeline
        user_stats.last_read = last_read
        self.env.cache.set_last_read_in_group_for_user(group_id, user_id, to_ts(last_read))

        # recount unread from cassandra and save in cache and db
        self.env.cache.clear_unread_in_group_for_user(group_id, user_id)
        user_stats.unread_count = self.env.storage.get_unread_in_group(group_id, user_id, last_read)

        # updating unread removes bookmark, and bookmark always counts as 1 unread
        decrease_by = unread_count_before_changing
        if user_stats.bookmark:
            decrease_by = 1

        self.env.cache.remove_unread_group(user_id, group_id)
        self.env.cache.decrease_total_unread_message_count(user_id, decrease_by)

        # highlight time is removed if a user reads a conversation
        user_stats.highlight_time = self.handler.long_ago
        if that_user_stats is not None:
            that_user_stats.receiver_highlight_time = self.handler.long_ago

    def _set_bookmark(
            self,
            group_id: str,
            user_id: int,
            user_stats: UserGroupStatsEntity,
            query: UpdateUserGroupStats
    ) -> None:
        user_stats.bookmark = query.bookmark

        if query.bookmark:
            with self.env.cache.pipeline() as p:
                self.env.cache.increase_total_unread_message_count([user_id], 1, pipeline=p)
                self.env.cache.add_unread_group([user_id], group_id, pipeline=p)
        # doesn't work well with pipeline
        else:
            # bookmark always counts as 1 unread, so just decrease by 1
            self.env.cache.decrease_total_unread_message_count(user_id, 1)
            self.env.cache.remove_unread_group(user_id, group_id)

    def update(
            self,
            group_id: str,
            user_id: int,
            query: UpdateUserGroupStats,
            db: Session
    ) -> None:
        user_stats, that_user_stats, group = self.handler.get_both_user_stats_in_group(group_id, user_id, query, db)

        unread_count_before_changing = user_stats.unread_count
        last_read = to_dt(query.last_read_time, allow_none=True)
        delete_before = to_dt(query.delete_before, allow_none=True)
        highlight_time = to_dt(
            query.highlight_time, allow_none=True
        )
        now = utcnow_dt()

        if user_stats is None:
            raise UserNotInGroupException(
                f"tried to update group stats for user {user_id} not in group {group_id}"
            )

        # used by apps to sync changes
        user_stats.last_updated_time = now

        if query.bookmark is not None:
            self._set_bookmark(group_id, user_id, user_stats, query)

            # set the last read time to now(), since a user can't remove a
            # bookmark without opening the conversation
            if query.bookmark is False and query.last_read_time is None:
                return now

        if query.pin is not None:
            user_stats.pin = query.pin

        if query.rating is not None:
            user_stats.rating = query.rating

        if last_read is not None:
            self._set_last_read(
                group_id, user_id, last_read, unread_count_before_changing, group, user_stats, that_user_stats, db
            )

        if delete_before is not None:
            self._set_delete_before(group_id, user_id, user_stats, delete_before, unread_count_before_changing, db)

        # can't set highlight time if also setting last read time
        if highlight_time is not None and last_read is None:
            self._set_highlight_time(group_id, user_id, user_stats, that_user_stats, highlight_time, query, db)

        elif query.hide is not None:
            self._set_hide(group_id, user_id, user_stats, unread_count_before_changing, query)

        if query.hide is not None or query.delete_before is not None:
            self.env.cache.reset_count_group_types_for_user(user_id)

        db.add(user_stats)
        db.commit()
