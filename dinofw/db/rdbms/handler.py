import datetime
import json
from datetime import datetime as dt
from typing import Dict, Union
from typing import List
from typing import Optional
from typing import Tuple
from uuid import uuid4 as uuid

import arrow
from loguru import logger
from pydantic.fields import defaultdict
from sqlalchemy import case
from sqlalchemy import func
from sqlalchemy import literal
from sqlalchemy import distinct
from sqlalchemy import or_
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import load_only

from dinofw.db.rdbms.handler_stats import UpdateUserGroupStatsHandler
from dinofw.db.rdbms.models import GroupEntity, DeletedStatsEntity
from dinofw.db.rdbms.models import UserGroupStatsEntity
from dinofw.db.rdbms.schemas import GroupBase, DeletedStatsBase
from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.queries import CreateGroupQuery, PublicGroupQuery, SendMessageQuery, ActionLogQuery
from dinofw.rest.queries import GroupQuery
from dinofw.rest.queries import GroupUpdatesQuery
from dinofw.rest.queries import UpdateGroupQuery
from dinofw.rest.queries import UpdateUserGroupStats
from dinofw.utils import group_id_to_users, to_dt, truncate_json_message, is_none_or_zero, is_non_zero
from dinofw.utils import split_into_chunks
from dinofw.utils import to_ts
from dinofw.utils import trim_micros
from dinofw.utils import users_to_group_id
from dinofw.utils import utcnow_dt
from dinofw.utils import utcnow_ts
from dinofw.utils.config import GroupTypes, ConfigKeys, GroupStatus, MessageTypes
from dinofw.utils.exceptions import NoSuchGroupException, NoSuchUserException, UserStatsOrGroupAlreadyCreated
from dinofw.utils.exceptions import UserNotInGroupException
from dinofw.utils.perf import time_coroutine


def filter_whisper_users_if_any(
    all_users: List[UserGroupStatsEntity], message: MessageBase, context: Optional[str] = None
) -> (List[UserGroupStatsEntity], bool):
    """
    If the message is a whisper, filter out users that are not mentioned in the message.

    Example payload:

        {
            "message_payload": "{"content":" - vipleo abccc","msg_group_type":2}",
            "message_type": 0,
            "mention_user_ids": [],
            "context": "{"action":64,"whisper":[{"id":5000441,"nickname":"vipleo"},{"id":5000440,"nickname":"
            premiumleo"}]}"
        }
    """
    if context is None or message.message_type != 0:
        return all_users, False

    try:
        context = json.loads(context)
    except json.decoder.JSONDecodeError as e:
        logger.error(f"Failed to decode context to check for whispers. Context was '{context}', error: {e}")
        return all_users, False

    if context.get("action") != MessageTypes.ACTION_WHISPER:
        return all_users, False

    mentioned_ids = {
        user.get("id", None)
        for user in context.get("whisper", [])
    }
    mentioned_ids = {
        int(user_id)
        for user_id in mentioned_ids if user_id is not None
    }

    return [
        user for user in all_users
        if user.user_id in mentioned_ids
    ], True


def update_statement_for_deleted_flag(statement, deleted: bool):
    if deleted is False:
        statement = statement.filter(
            GroupEntity.status.in_(GroupStatus.visible_statuses),
            UserGroupStatsEntity.deleted.is_(False),
            UserGroupStatsEntity.delete_before < GroupEntity.updated_at
        )
    elif deleted is True:
        statement = statement.filter(
            UserGroupStatsEntity.deleted.is_(True),
            UserGroupStatsEntity.delete_before >= GroupEntity.updated_at,

            # if a user is kicked then deletes the group, we don't want to include it
            UserGroupStatsEntity.kicked.is_(False)
        )
    else:
        # 'query.deleted==None' means to include both deleted and non-deleted groups
        statement = statement.filter(
            # but still ignore kicked groups
            or_(
                and_(
                    UserGroupStatsEntity.deleted.is_(True),
                    UserGroupStatsEntity.kicked.is_(False)
                ),
                UserGroupStatsEntity.deleted.is_(False)
            )
        )

    return statement


class RelationalHandler:
    def __init__(self, env):
        self.env = env
        self.stats_handler = UpdateUserGroupStatsHandler(env, self)

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = arrow.get(beginning_of_1995).datetime

        self.room_max_history_days = int(float(env.config.get(
            ConfigKeys.ROOM_MAX_HISTORY_DAYS,
            domain=ConfigKeys.HISTORY,
            default=30
        )))
        self.room_max_history_count = int(float(env.config.get(
            ConfigKeys.ROOM_MAX_HISTORY_COUNT,
            domain=ConfigKeys.HISTORY,
            default=500
        )))

    async def get_public_group_ids(self, db: AsyncSession) -> List[str]:

        groups = await db.run_sync(lambda _db: (
            _db.query(GroupEntity.group_id)
            .filter(
                GroupEntity.group_type == GroupTypes.PUBLIC_ROOM
            )
            .all()
        ))

        return [group[0] for group in groups]

    async def get_public_groups(self, query: PublicGroupQuery, db: AsyncSession) -> List[GroupBase]:
        # TODO: check this, why cache group ids and do IN query? just use group types?
        """
        public_group_ids = self.env.cache.get_public_group_ids()
        if public_group_ids is None or not len(public_group_ids):
            group_ids = self.get_public_group_ids(db)

            if len(group_ids):
                self.env.cache.add_public_group_ids(group_ids)
        """

        statement = await db.run_sync(lambda _db:
            _db.query(GroupEntity)
            .filter(
                # GroupEntity.group_id.in_(public_group_ids)
                GroupEntity.group_type.in_(GroupTypes.public_group_types),
                GroupEntity.status != GroupStatus.DELETED
            )
        )

        # "rooms my friends are in"
        if query.users and len(query.users):
            statement = await db.run_sync(lambda _db:
                statement.join(
                    UserGroupStatsEntity,
                    UserGroupStatsEntity.group_id == GroupEntity.group_id,
                ).filter(
                    UserGroupStatsEntity.user_id.in_(query.users)
                ).distinct()
            )  # TODO: test distinct

        if query.spoken_languages is not None and len(query.spoken_languages):
            spoken_languages = [
                lang.lower() for lang in query.spoken_languages
                if type(lang) is str and len(lang) == 2 and lang.isascii()
            ]
            statement = statement.filter(
                GroupEntity.language.in_(spoken_languages)
            )

        if query.include_archived and is_non_zero(query.admin_id):
            # include both archived and non-archived groups
            pass
        else:
            # otherwise only non-archived groups
            statement = statement.filter(
                GroupEntity.status != GroupStatus.ARCHIVED
            )

        group_entities = await db.run_sync(lambda _db: statement.all())

        return [
            GroupBase(**group_entity.__dict__)
            for group_entity in group_entities
        ]

    async def get_users_in_group(
            self,
            group_id: str,
            db: AsyncSession,
            include_group: bool = True
    ) -> (Optional[GroupBase], Optional[Dict[int, float]], Optional[int]):
        group = None

        # not always needed
        if include_group:
            group_entity = await db.run_sync(lambda _db:
                _db.query(GroupEntity)
                .filter(GroupEntity.group_id == group_id)
                .first()
            )

            if group_entity is None:
                raise NoSuchGroupException(group_id)
            group = GroupBase(**group_entity.__dict__)

        users_and_join_time = await self.get_user_ids_and_join_time_in_group(group_id, db)
        user_count = len(users_and_join_time)

        return group, users_and_join_time, user_count

    async def get_last_sent_for_user(self, user_id: int, db: AsyncSession) -> (str, float):
        group_id, last_sent = await self.env.cache.get_last_sent_for_user(user_id)
        if group_id is not None:
            return group_id, last_sent

        group_id_and_last_sent = await db.run_sync(lambda _db:
            _db.query(
                UserGroupStatsEntity.group_id,
                UserGroupStatsEntity.last_sent
            )
            .filter(UserGroupStatsEntity.user_id == user_id)
            .order_by(UserGroupStatsEntity.last_sent)
            .limit(1)
            .first()
        )

        if group_id_and_last_sent is None:
            return None, None

        group_id, last_sent = group_id_and_last_sent
        last_sent = to_ts(last_sent)
        await self.env.cache.set_last_sent_for_user(user_id, group_id, last_sent)

        return group_id, last_sent

    # noinspection PyMethodMayBeStatic
    async def get_group_id_type_join_time_for_user(self, user_id: int, db: AsyncSession) -> List[Tuple[str, int, dt]]:
        """
        No restrictions. Used for data exports. Only called internally.
        """
        groups = await db.run_sync(lambda _db:
            _db.query(
                GroupEntity.group_id,
                GroupEntity.group_type,
                UserGroupStatsEntity.join_time
            )
            .join(
                UserGroupStatsEntity,
                UserGroupStatsEntity.group_id == GroupEntity.group_id,
            )
            .filter(
                UserGroupStatsEntity.user_id == user_id
            )
            .all()
        )

        return groups

    # noinspection PyMethodMayBeStatic
    async def get_group_ids_and_created_at_for_user(self, user_id: int, db: AsyncSession) -> List[Tuple[str, dt]]:
        groups = await db.run_sync(lambda _db:
            _db.query(
                GroupEntity.group_id,
                GroupEntity.created_at,
            )
            .join(
                UserGroupStatsEntity,
                UserGroupStatsEntity.group_id == GroupEntity.group_id,
            )
            .filter(
                UserGroupStatsEntity.user_id == user_id
            )
            .all()
        )

        return groups

    async def get_groups_for_user(
        self,
        user_id: int,
        query: GroupQuery,
        db: AsyncSession
    ) -> List[UserGroupBase]:
        """
        what we're doing:

        select * from
            groups g
        inner join
            user_group_stats u on u.group_id = g.group_id
        where
            u.user_id = 4444 and
            u.hide = false and
            u.deleted = false and
            g.status in (0, -1) and
            u.delete_before < g.updated_at and
            g.last_message_time < now()
        order by
            u.pin desc,
            greatest(u.highlight_time, g.last_message_time) desc
        limit 10;

            g.archived = false and
            g.deleted = false and
            (u.unread_count > 0 or u.bookmark = true)
            ((u.last_read < g.last_message_time) or u.bookmark = true)
        """
        @time_coroutine(logger, "get_groups_for_user(): query groups")
        async def query_groups():
            until = to_dt(query.until)

            statement = await db.run_sync(lambda _db:
                _db.query(
                    GroupEntity,
                    UserGroupStatsEntity
                )
                .join(
                    UserGroupStatsEntity,
                    UserGroupStatsEntity.group_id == GroupEntity.group_id
                )
                .filter(
                    GroupEntity.last_message_time < until,
                    UserGroupStatsEntity.user_id == user_id
                )
            )

            # "include deleted? yes/no/both" based on the query.deleted parameter
            statement = update_statement_for_deleted_flag(statement, query.deleted)

            if query.hidden is not None:
                statement = statement.filter(
                    UserGroupStatsEntity.hide.is_(query.hidden)
                )

            if query.group_type is None:
                statement = statement.filter(
                    GroupEntity.group_type.in_(GroupTypes.private_group_types)
                )
            else:
                statement = statement.filter(
                    GroupEntity.group_type == query.group_type
                )

            if query.only_unread:
                statement = statement.filter(
                    or_(
                        # with 1 unread, last_read == last_message_time; in cassandra we add random MS
                        # to creation time, but migration is too slow to use the same time for
                        # last_message_time in postgres, so use unread_count instead
                        # UserGroupStatsEntity.last_read < GroupEntity.last_message_time,
                        UserGroupStatsEntity.unread_count > 0,
                        UserGroupStatsEntity.bookmark.is_(True)
                    )
                )

            # generate the 1-to-1 group ids based on the receiver ids in the query
            if query.receiver_ids:
                group_ids = [
                    users_to_group_id(user_id, receiver_id)
                    for receiver_id in query.receiver_ids
                ]
                statement = statement.filter(
                    GroupEntity.group_type == GroupTypes.ONE_TO_ONE,
                    UserGroupStatsEntity.group_id.in_(group_ids)
                )

            statement = (
                statement.order_by(
                    UserGroupStatsEntity.pin.desc(),
                    func.greatest(
                        UserGroupStatsEntity.highlight_time,
                        GroupEntity.last_message_time,
                    ).desc(),
                )
                .limit(query.per_page)
            )

            return await db.run_sync(lambda _db: statement.all())

        results = await query_groups()
        receiver_stats_base = await self.get_receiver_stats(results, user_id, query.receiver_stats, db)

        return await self.format_group_stats_and_count_unread(
            db,
            results,
            receiver_stats=receiver_stats_base,
            user_id=user_id,
            query=query
        )

    async def count_total_unread(self, user_id: int, db: AsyncSession) -> (int, List[str]):
        """
        count all unread messages for a user, including bookmarked groups

        postgres query:

            select
                coalesce(
                    sum(unread_count)
                    filter (where bookmark = false and notifications = true),
                0) +
                coalesce(
                    sum(mentions)
                    filter (where bookmark = false and notifications = false),
                0) +
                coalesce(
                    count(1) filter (where bookmark = true),
                0) as unread_count,
                count(distinct group_id) as n_unread_groups
            from
                user_group_stats
            where
                user_id = 8888 and
                hide = false and
                deleted = false and
                (
                    bookmark = true or
                    unread_count > 0 or
                    mentions > 0
                );
        """
        unread_count = await db.run_sync(lambda _db:
            _db.query(
                func.coalesce(
                    func.sum(UserGroupStatsEntity.unread_count).filter(
                        UserGroupStatsEntity.bookmark.is_(False),
                        UserGroupStatsEntity.notifications.is_(True)
                    ), 0
                ) +
                func.coalesce(
                    func.sum(UserGroupStatsEntity.mentions).filter(
                        UserGroupStatsEntity.notifications.is_(False)
                    ), 0
                ) +
                func.coalesce(
                    func.count(1).filter(UserGroupStatsEntity.bookmark.is_(True)), 0
                )
            )
            .filter(
                UserGroupStatsEntity.user_id == user_id,
                UserGroupStatsEntity.hide.is_(False),
                UserGroupStatsEntity.deleted.is_(False),
                or_(
                    UserGroupStatsEntity.bookmark.is_(True),
                    UserGroupStatsEntity.unread_count > 0,
                    UserGroupStatsEntity.mentions > 0
                )
            )
            .first()
        )

        unread_group_ids = await db.run_sync(lambda _db:
            _db.query(
                UserGroupStatsEntity
            )
            .filter(
                UserGroupStatsEntity.user_id == user_id,
                UserGroupStatsEntity.hide.is_(False),
                UserGroupStatsEntity.deleted.is_(False),
                or_(
                    # any bookmarked group counts as having unread messages
                    UserGroupStatsEntity.bookmark.is_(True),
                    and_(
                        # real unread count only counts as unread if notifications are enabled
                        UserGroupStatsEntity.notifications.is_(True),
                        UserGroupStatsEntity.unread_count > 0
                    ),
                    and_(
                        # if they're not enabled, the user must have been mentioned to count as an unread group
                        UserGroupStatsEntity.notifications.is_(False),
                        UserGroupStatsEntity.mentions > 0
                    )
                )
            )
            .options(load_only("group_id"))
            .all()
        )

        unread_group_ids = [
            unread_group.group_id for unread_group in unread_group_ids
        ]

        # if the user has NO groups, sqlalchemy will return None not 0
        if unread_count[0] is None:
            unread_count = (0,)

        return unread_count[0], unread_group_ids

    async def get_groups_updated_since(
        self,
        user_id: int,
        query: GroupUpdatesQuery,
        db: AsyncSession,
        public_only: bool = False
    ):
        """
        the only difference between get_groups_for_user() and get_groups_updated_since() is
        that this method doesn't care about last_message_time, hide, delete_before, since
        this method is used to sync changed to different devices. This method is also
        filtering by "since" instead of "until", because for syncing we're paginating
        "forwards" instead of "backwards"
        """
        @time_coroutine(logger, "get_groups_updated_since(): query groups")
        async def query_groups():
            since = to_dt(query.since)
            until = None
            if query.until is not None and query.until > 0:
                until = to_dt(query.until)

            statement = await db.run_sync(lambda _db:
                _db.query(GroupEntity, UserGroupStatsEntity)
                .filter(
                    GroupEntity.group_id == UserGroupStatsEntity.group_id,
                    UserGroupStatsEntity.user_id == user_id,
                    UserGroupStatsEntity.last_updated_time > since,
                )
            )

            if until is not None:
                statement = statement.filter(
                    UserGroupStatsEntity.last_updated_time <= until
                )

            if public_only:
                statement = statement.filter(
                    GroupEntity.group_type.in_(GroupTypes.public_group_types)
                )
            else:
                statement = statement.filter(
                    GroupEntity.group_type.in_(GroupTypes.private_group_types)
                )

            return await db.run_sync(lambda _db:
                statement.order_by(
                    UserGroupStatsEntity.pin.desc(),
                    func.greatest(
                        UserGroupStatsEntity.highlight_time,
                        GroupEntity.last_message_time,
                    ).desc(),
                )
                .limit(query.per_page)
                .all()
            )

        results = await query_groups()
        receiver_stats = await self.get_receiver_stats(results, user_id, query.receiver_stats, db)

        return await self.format_group_stats_and_count_unread(
            db,
            results,
            receiver_stats=receiver_stats,
            user_id=user_id,
            query=query
        )

    @time_coroutine(logger, "get_receiver_stats()")
    async def get_receiver_stats(self, results, user_id, receiver_stats: bool, db: AsyncSession):
        if not receiver_stats:
            return list()

        group_ids = [g.group_id for g, _ in results if g.group_type == GroupTypes.ONE_TO_ONE]
        if len(group_ids):
            return await self.get_receiver_user_stats(group_ids, user_id, db)

        return list()

    # noinspection PyMethodMayBeStatic
    async def get_receiver_user_stats(self, group_ids: List[str], user_id: int, db: AsyncSession):
        return await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(
                UserGroupStatsEntity.group_id.in_(group_ids),
                UserGroupStatsEntity.user_id != user_id,
            )
            .all()
        )

    @time_coroutine(logger, "format_group_stats_and_count_unread()")
    async def format_group_stats_and_count_unread(
        self,
        db: AsyncSession,
        results: List[Tuple[GroupEntity, UserGroupStatsEntity]],
        receiver_stats: List[UserGroupStatsEntity],
        user_id: int,
        query: GroupQuery
    ) -> List[UserGroupBase]:
        def count_for_group(_group, _stats):
            _unread_count = -1
            _receiver_unread_count = -1

            # only count for receiver if it's a 1v1 group
            if query.receiver_stats and _group.group_type == GroupTypes.ONE_TO_ONE:
                user_a, user_b = group_id_to_users(_group.group_id)
                user_to_count_for = (
                    user_a if user_b == user_id else user_b
                )

                if _group.group_id in receivers:
                    _receiver_unread_count = receivers[_group.group_id].unread_count
                else:
                    e_msg = f"no receiver stats user {user_to_count_for} group {_group.group_id}, deleted profile?"
                    logger.warning(e_msg)

            if query.count_unread:
                _unread_count = _stats.unread_count
                if _stats.bookmark:
                    _unread_count += 1

            return _unread_count, _receiver_unread_count

        receivers = dict()
        for stat in receiver_stats:
            receivers[stat.group_id] = UserGroupStatsBase(**stat.__dict__)

        # batch all redis/db queries for join times
        group_users_join_time = await self.get_user_ids_and_join_time_in_groups(
            [group.group_id for group, user_stats in results],
            db
        )

        groups = list()
        for group_entity, user_group_stats_entity in results:
            group = GroupBase(**group_entity.__dict__)
            user_group_stats = UserGroupStatsBase(**user_group_stats_entity.__dict__)

            unread_count, receiver_unread_count = count_for_group(group, user_group_stats)

            receiver_stat = None
            if group.group_id in receivers:
                receiver_stat = receivers[group.group_id]

            join_times = group_users_join_time.get(group_entity.group_id, dict())
            user_group = UserGroupBase(
                group=group,
                user_stats=user_group_stats,
                user_join_times=join_times,
                user_count=len(join_times),
                unread=unread_count,
                receiver_unread=receiver_unread_count,
                receiver_user_stats=receiver_stat,
            )
            groups.append(user_group)

        return groups

    async def update_group_new_message(
        self,
        message: MessageBase,
        db: AsyncSession,
        sender_user_id: int,
        update_unread_count: bool = True,
        update_last_message: bool = True,
        update_last_message_time: bool = True,
        update_group_updated_at: bool = True,
        unhide_group: bool = True,
        mentions: List[int] = None,
        context: Optional[str] = None
    ) -> GroupBase:
        async def get_receivers() -> List[UserGroupStatsEntity]:
            # if a group is hidden, it might have unread messages when it was hidden, so we have
            # to query for it and restore the original amount plus one (this message)
            return await db.run_sync(lambda _db:
                _db.query(UserGroupStatsEntity)
                .filter(
                    UserGroupStatsEntity.group_id == message.group_id,
                    UserGroupStatsEntity.user_id != sender_user_id,
                    UserGroupStatsEntity.kicked.is_(False)
                )
                .all()
            )

        async def update_cached_unread():
            async with self.env.cache.pipeline() as p:
                # for knowing if we need to send read-receipts when user opens a conversation
                await self.env.cache.set_last_message_time_in_group(
                    message.group_id,
                    to_ts(sent_time),
                    pipeline=p
                )

                if len(user_to_hidden_stats):
                    for user_id in user_to_hidden_stats.keys():
                        # if the group had unread and then notifications were disabled and
                        # then was hidden, don't restore unread count on new message
                        if user_id not in user_ids_with_notification_on:
                            continue

                        amount = user_to_hidden_stats[user_id].unread_count + 1
                        await self.env.cache.increase_total_unread_message_count([user_id], amount, pipeline=p)
                else:
                    # update total unread count for all users that have notifications enabled
                    await self.env.cache.increase_total_unread_message_count(user_ids_with_notification_on, 1, pipeline=p)

                # unread in THIS group should increase whether notifications are on or off
                await self.env.cache.increase_unread_in_group_for(message.group_id, non_sender_user_ids, pipeline=p)
                await self.env.cache.add_unread_group(non_sender_user_ids, message.group_id, pipeline=p)

                # if notifications are disabled BUT the user was mentioned, increase the total unread count anyway
                if mentions and len(mentions):
                    for mention_user_id in mentions:
                        if mention_user_id not in user_ids_with_notification_on:
                            await self.env.cache.increase_total_unread_message_count([mention_user_id], 1, pipeline=p)

        async def increase_mentions():
            await db.run_sync(lambda _db:
                _db.query(UserGroupStatsEntity)
                .filter(
                    UserGroupStatsEntity.group_id == group.group_id,
                    UserGroupStatsEntity.user_id.in_(mentions),
                    UserGroupStatsEntity.kicked.is_(False)
                )
                .update({
                    UserGroupStatsEntity.mentions: UserGroupStatsEntity.mentions + 1
                }, synchronize_session=False)
            )

        async def update_sent_message_count_in_cache():
            previous_sent_count = await self._get_then_update_sent_count(message.group_id, sender_user_id, db)

            # previously we increase unread for all; now set to 0 for the sender, since
            # it won't be unread for him/her
            #
            # also only update 'sent_message_count' if it's been previously counted using
            # the /count api (-1 means it has not been counted in cassandra yet)
            if previous_sent_count == -1:
                await db.run_sync(lambda _db: _db.query(UserGroupStatsEntity).filter(
                    UserGroupStatsEntity.group_id == message.group_id,
                    UserGroupStatsEntity.user_id == sender_user_id
                ).update({
                    UserGroupStatsEntity.unread_count: 0
                }))
            else:
                await db.run_sync(lambda _db: _db.query(UserGroupStatsEntity).filter(
                    UserGroupStatsEntity.group_id == message.group_id,
                    UserGroupStatsEntity.user_id == sender_user_id
                ).update({
                    UserGroupStatsEntity.unread_count: 0,
                    UserGroupStatsEntity.sent_message_count: previous_sent_count + 1
                }))

        async def update_last_message_in_db():
            # sometimes we don't want to change the order of conversations on action log creation
            if update_last_message_time:
                group.last_message_time = sent_time

            group.last_message_id = message.message_id
            group.last_message_type = message.message_type
            group.last_message_user_id = message.user_id

            # db limit is 65k (text field); some messages could be ridiculously long
            group.last_message_overview = truncate_json_message(
                message.message_payload,
                limit=600,  # TODO: wait with changing field type to text, keep at 1024 limit varchar for now
                only_content=True  # column changed to text, can save everything
            )

        async def update_unread_count_in_db_and_unhide():
            # when creating action logs, we want to sync changes to apps, but not necessarily un-hide a group
            if update_unread_count:
                await db.run_sync(lambda _db: statement.update({
                    UserGroupStatsEntity.last_updated_time: sent_time,
                    UserGroupStatsEntity.unread_count: UserGroupStatsEntity.unread_count + 1,
                    UserGroupStatsEntity.deleted: False
                }))
            else:
                await db.run_sync(lambda _db: statement.update({
                    UserGroupStatsEntity.last_updated_time: sent_time,
                }))

            if unhide_group:
                await self.env.cache.set_hide_group(group.group_id, False)
                await db.run_sync(lambda _db: statement.update({
                    UserGroupStatsEntity.hide: False
                }))

        group = await db.run_sync(lambda _db:
            _db.query(GroupEntity)
            .filter(GroupEntity.group_id == message.group_id)
            .first()
        )
        sent_time = message.created_at
        is_whisper = False
        receivers_in_group = None

        if group is None:
            raise NoSuchGroupException(message.group_id)

        # some action logs don't need to update these
        if update_unread_count:
            receivers_in_group = await get_receivers()
            receivers_in_group, is_whisper = filter_whisper_users_if_any(receivers_in_group, message, context)

            non_sender_user_ids = [
                user.user_id
                for user in receivers_in_group
            ]
            user_to_hidden_stats = {
                user.user_id: user
                for user in receivers_in_group
                if user.hide
            }
            user_ids_with_notification_on = {
                user.user_id
                for user in receivers_in_group
                if user.notifications
            }

            await update_cached_unread()

        # some action logs don't need to update last message
        if update_last_message and not is_whisper:
            await update_last_message_in_db()

        # always update this, unless it's a nickname change or some other action log
        if update_group_updated_at:
            group.updated_at = sent_time

        # we have to count the number of mentions; it's reset when the user reads/opens the conversation
        if mentions and len(mentions):
            await increase_mentions()

        statement = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(
                UserGroupStatsEntity.group_id == group.group_id,
                UserGroupStatsEntity.user_id != sender_user_id,
                UserGroupStatsEntity.kicked.is_(False)
            )
        )

        if is_whisper and receivers_in_group is not None:
            # if the message is a whisper, we only want to update the users that are mentioned in the message
            statement = statement.filter(
                UserGroupStatsEntity.user_id.in_([user.user_id for user in receivers_in_group])
            )

        await update_unread_count_in_db_and_unhide()
        await update_sent_message_count_in_cache()

        group_base = GroupBase(**group.__dict__)

        db.add(group)
        await db.commit()

        return group_base

    async def _get_then_update_sent_count(self, group_id, user_id, db):
        async def update_cache_value(_sent_count):
            # the db default value is -1, so even if it's -1, set it in the cache so that we
            # don't have to check the db for every new message
            if sent_count == -1:
                await self.env.cache.set_sent_message_count_in_group_for_user(group_id, user_id, sent_count)

            # if it's been counted before, increase by one in cache since the user is now
            # sending a new message
            else:
                await self.env.cache.set_sent_message_count_in_group_for_user(group_id, user_id, sent_count + 1)

        # first check the cache
        sent_count = await self.env.cache.get_sent_message_count_in_group_for_user(group_id, user_id)

        # count be -1, which means we've checked the db before, and it has not yet been
        # counted from cassandra, in which case we'll skip increasing the sent count for
        # this new message, since we don't know yet how many messages the user has sent
        # previously without counting in cassandra first
        if sent_count is not None:
            await update_cache_value(sent_count)
            return sent_count

        # then check the db
        sent_count = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity.sent_message_count)
            .filter(UserGroupStatsEntity.group_id == group_id)
            .filter(UserGroupStatsEntity.user_id == user_id)
            .first()
        )

        if sent_count is None:
            sent_count = -1
        else:
            sent_count = sent_count[0]

        await update_cache_value(sent_count)
        return sent_count

    async def get_last_read_for_user(self, group_id: str, user_id: int, db: AsyncSession) -> Dict[int, float]:
        last_read = await self.env.cache.get_last_read_in_group_for_user(group_id, user_id)
        if last_read is not None:
            return {user_id: last_read}

        last_reads = await db.run_sync(lambda _db:
            _db.query(
                UserGroupStatsEntity.last_read
            )
            .filter(
                UserGroupStatsEntity.group_id == group_id,
                UserGroupStatsEntity.user_id == user_id
            )
            .first()
        )

        if last_reads is None or len(last_reads) == 0:
            raise UserNotInGroupException(
                f"no stats entity found in group {group_id} for {user_id}"
            )

        last_read = to_ts(last_reads[0])
        await self.env.cache.set_last_read_in_group_for_user(group_id, user_id, last_read)

        return {user_id: last_read}

    async def remove_user_stats_for_offline_users(self, user_ids: List[int], db: AsyncSession) -> None:
        logger.info("removing user stats for users (went offline): {user_ids}")

        groups = await db.run_sync(lambda _db:
            _db.query(
                GroupEntity.group_id,
                GroupEntity.group_type,
                UserGroupStatsEntity.user_id
            )
            .join(
                UserGroupStatsEntity,
                UserGroupStatsEntity.group_id == GroupEntity.group_id
            )
            .filter(
                UserGroupStatsEntity.user_id.in_(user_ids),
                GroupEntity.group_type.in_(GroupTypes.public_group_types)
            )
            .all()
        )

        user_to_group_and_type = dict()
        for group_id, group_type, user_id in groups:
            if user_id not in user_to_group_and_type:
                user_to_group_and_type[user_id] = dict()

            user_to_group_and_type[user_id][group_id] = group_type

        for user_id in user_ids:
            group_id_to_type = user_to_group_and_type.get(user_id, dict())
            group_ids = list(group_id_to_type.keys())

            await self.copy_to_deleted_groups_table(group_id_to_type, user_id, db, skip_public=False)
            await self.remove_user_group_stats_for_user(group_ids, user_id, db)
            await self.env.cache.reset_count_group_types_for_user(user_id)


    async def get_online_users(self, db: AsyncSession) -> List[int]:
        # don't actually expire the online list, since that one is
        # used in other places to check individual users
        expired = await self.env.cache.get_online_users_ttl_expired()

        if not expired:
            online = await self.env.cache.get_online_users()
            if online is not None and len(online):
                return online

        # also sync with the users in the db, since those are the ones that will be shown as dangling
        # if we miss them when syncing from the offline detector; this API is also only called once
        # every 5min or so, so it's not a big deal to have a big select query for it
        online = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity.user_id)
            .filter(UserGroupStatsEntity.kicked.is_(False))
            .all()
        )

        if online and len(online):
            online = [user[0] for user in online]
        else:
            online = list()

        await self.env.cache.set_online_users_only(online)
        return online

    async def get_last_reads_in_group(self, group_id: str, db: AsyncSession) -> Dict[int, float]:
        users = await self.env.cache.get_last_read_times_in_group(group_id)
        if users is not None:
            return users

        users = await db.run_sync(lambda _db:
            _db.query(
                UserGroupStatsEntity.user_id,
                UserGroupStatsEntity.last_read
            )
            .filter(UserGroupStatsEntity.group_id == group_id)
            .all()
        )

        if users is None or len(users) == 0:
            return dict()

        user_ids_last_read = {user[0]: to_ts(user[1]) for user in users}
        await self.env.cache.set_last_read_in_group_for_users(group_id, user_ids_last_read)

        return user_ids_last_read

    async def get_deleted_groups_for_user(self, user_id: int, db: AsyncSession) -> List[DeletedStatsBase]:
        deleted_stats = await db.run_sync(lambda _db:
            _db.query(
                DeletedStatsEntity
            )
            .filter(
                DeletedStatsEntity.user_id == user_id
            )
            .all()
        )

        return [
            DeletedStatsBase(**deleted_stats_entity.__dict__)
            for deleted_stats_entity in deleted_stats
        ]

    async def get_group_types(self, group_ids: List[str], db: AsyncSession) -> Dict[str, int]:
        group_types = await db.run_sync(lambda _db:
            _db.query(
                GroupEntity.group_id,
                GroupEntity.group_type
            )
            .filter(GroupEntity.group_id.in_(group_ids))
            .all()
        )

        return {
            group_id: group_type
            for group_id, group_type in group_types
        }

    async def copy_to_deleted_groups_table(
        self, group_id_to_type: Dict[str, int], user_id: int, db: AsyncSession, skip_public: bool = True
    ) -> None:
        if skip_public:
            # don't create deletion records for public groups, users will join and leave them all the time
            group_ids = [
                group_id for group_id, group_type in group_id_to_type.items()
                if group_type != GroupTypes.PUBLIC_ROOM
            ]
        else:
            group_ids = list(group_id_to_type.keys())

        groups_to_copy = await db.run_sync(lambda _db:
            _db.query(
                UserGroupStatsEntity.group_id,
                UserGroupStatsEntity.join_time
            )
            .filter(
                UserGroupStatsEntity.group_id.in_(group_ids),
                UserGroupStatsEntity.user_id == user_id
            )
            .all()
        )

        delete_time = utcnow_dt()
        for group_id, join_time in groups_to_copy:
            # save a copy of the entry to be deleted; sometimes we have to be
            # able to access message history of deleted users, for legal cases
            deleted_entity = DeletedStatsEntity(
                user_id=user_id,
                group_id=group_id,
                join_time=join_time,
                delete_time=delete_time,
                group_type=group_id_to_type.get(group_id)
            )
            db.add(deleted_entity)

        await db.commit()

    async def remove_user_group_stats_for_user(
        self, group_ids: List[str], user_id: int, db: AsyncSession
    ) -> None:
        """
        called when a user leaves a group
        """
        """
        # need to count how much to decrease the cached total unread count with
        unread_count = await db.run_sync(lambda _db:
            _db.query(
                func.sum(UserGroupStatsEntity.unread_count).filter(UserGroupStatsEntity.bookmark.is_(False)) +
                func.count(1).filter(UserGroupStatsEntity.bookmark.is_(True))
            )
            .filter(
                UserGroupStatsEntity.user_id == user_id,
                UserGroupStatsEntity.hide.is_(False),
                UserGroupStatsEntity.deleted.is_(False),
                UserGroupStatsEntity.group_id.in_(group_ids),
                or_(
                    UserGroupStatsEntity.bookmark.is_(True),
                    UserGroupStatsEntity.unread_count > 0
                )
            )
            .first()
        )

        # if the user has NO groups, sqlalchemy will return None not 0
        unread_count = unread_count[0]
        if unread_count is None:
            unread_count = 0

        # no pipeline for this one since we might have to run another query to adjust negative values
        if unread_count > 0:
            self.env.cache.decrease_total_unread_message_count(user_id, unread_count)
        """

        async with self.env.cache.pipeline() as p:
            for group_id in group_ids:
                await self.env.cache.remove_unread_group(user_id, group_id, pipeline=p)

            await self.env.cache.reset_total_unread_message_count(user_id, pipeline=p)  # TODO: decreasing seems buggy, sometimes gets negative, so just reset instead
            await self.env.cache.remove_last_read_in_group_for_user(group_ids, user_id, pipeline=p)
            await self.env.cache.remove_join_time_in_group_for_user(group_ids, user_id, pipeline=p)
            await self.env.cache.remove_user_id_and_join_time_in_groups_for_user(group_ids, user_id, pipeline=p)

        # delete the stats for this user in these groups
        await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(
                UserGroupStatsEntity.group_id.in_(group_ids),
                UserGroupStatsEntity.user_id == user_id
            )
            .delete(synchronize_session=False)
        )

        # reset owner if the user is one
        await db.run_sync(lambda _db:
            _db.query(GroupEntity)
            .filter(
                GroupEntity.group_id.in_(group_ids),
                GroupEntity.owner_id == user_id
            )
            .update({
                GroupEntity.owner_id: None
            }, synchronize_session=False)
        )

        await db.commit()

    async def get_groups_without_users(self, db: AsyncSession) -> List[GroupBase]:
        groups = await db.run_sync(lambda _db:
            _db.query(GroupEntity)
            .outerjoin(
                UserGroupStatsEntity,
                UserGroupStatsEntity.group_id == GroupEntity.group_id,
            )
            .filter(
                GroupEntity.group_type == 1,
                UserGroupStatsEntity.user_id.is_(None)
            )
            .all()
        )

        return [
            GroupBase(**group_entity.__dict__)
            for group_entity in groups
        ]

    async def get_existing_user_ids_out_of(self, user_ids: List[int], db: AsyncSession):
        users = await db.run_sync(lambda _db:
            _db.query(distinct(UserGroupStatsEntity.user_id))
            .filter(UserGroupStatsEntity.user_id.in_(user_ids))
            .all()
        )

        if not users or not len(users):
            return list()

        return {user[0] for user in users}

    async def create_stats_for(self, stats: List[UserGroupStatsBase], db: AsyncSession, dry: bool) -> None:
        """
        used for restoring stats when they've been incorrectly deleted by users removing their profiles
        """
        for stat in stats:
            logger.info(f"restoring stats for group {stat.group_id} and user {stat.user_id}")
            stat_entity = UserGroupStatsEntity(
                group_id=stat.group_id,
                user_id=stat.user_id,
                last_read=stat.last_read,
                delete_before=stat.delete_before,
                last_sent=stat.last_sent,
                join_time=stat.join_time,
                last_updated_time=stat.last_updated_time,
                hide=stat.hide,
                pin=stat.pin,
                deleted=stat.deleted,
                highlight_time=stat.highlight_time,
                receiver_highlight_time=stat.receiver_highlight_time,
                sent_message_count=stat.sent_message_count,
            )

            if not dry:
                db.add(stat_entity)

        if not dry:
            await db.commit()

    async def get_oldest_last_read_in_group(self, group_id: str, db: AsyncSession) -> Optional[float]:
        last_read = await self.env.cache.get_last_read_in_group_oldest(group_id)
        if last_read is not None:
            return last_read

        last_read = await db.run_sync(lambda _db:
            _db.query(
                func.min(UserGroupStatsEntity.last_read)
            )
            .filter(
                UserGroupStatsEntity.group_id == group_id
            )
            .first()
        )

        if last_read is None or not len(last_read):
            logger.warning(f"no oldest last_read in db or cache for group {group_id}")
            return

        last_read = to_ts(last_read[0])
        await self.env.cache.set_last_read_in_group_oldest(group_id, last_read)

        return last_read

    @time_coroutine(logger, "get_last_read_in_group_for_users()")
    async def get_last_read_in_group_for_users(
        self, group_id: str, user_ids: List[int], db: AsyncSession
    ) -> Dict[int, float]:
        # TODO: remove this, not needed anymore
        last_reads, not_cached = await self.env.cache.get_last_read_in_group_for_users(
            group_id, user_ids
        )

        # got everything from the cache
        if not len(not_cached):
            return last_reads

        reads = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .with_entities(
                UserGroupStatsEntity.user_id,
                UserGroupStatsEntity.last_read,
            )
            .filter(
                UserGroupStatsEntity.group_id == group_id,
                UserGroupStatsEntity.user_id.in_(not_cached),
            )
            .all()
        )

        for user_id, last_read in reads:
            last_read_float = to_ts(last_read)
            last_reads[user_id] = last_read_float

        await self.env.cache.set_last_read_in_group_for_users(
            group_id, last_reads
        )

        return last_reads

    # noinspection PyMethodMayBeStatic
    async def get_all_group_ids_and_types_for_user(self, user_id: int, db: AsyncSession) -> Dict:
        """
        used only when a user is deleting their profile, no need
        to cache it, shouldn't happen that often
        """
        groups = await db.run_sync(lambda _db:
            _db.query(
                UserGroupStatsEntity.group_id,
                GroupEntity.group_type
            )
            .join(
                GroupEntity,
                GroupEntity.group_id == UserGroupStatsEntity.group_id,
            )
            .filter(
                UserGroupStatsEntity.user_id == user_id
            )
            .all()
        )

        if groups is None or len(groups) == 0:
            return dict()

        return {group[0]: group[1] for group in groups}

    # noinspection PyMethodMayBeStatic
    async def get_group_from_id(self, group_id: str, db: AsyncSession) -> GroupBase:
        group = await db.run_sync(lambda _db:
            _db.query(GroupEntity)
            .filter(
                GroupEntity.group_id == group_id,
            )
            .first()
        )

        if group is None:
            raise NoSuchGroupException(group_id)

        return GroupBase(**group.__dict__)

    # noinspection PyMethodMayBeStatic
    async def get_group_for_1to1(
        self, user_a: int, user_b: int, db: AsyncSession
    ):
        group_id = users_to_group_id(user_a, user_b)

        group = await db.run_sync(lambda _db:
            _db.query(GroupEntity)
            .filter(
                GroupEntity.group_type == GroupTypes.ONE_TO_ONE,
                GroupEntity.group_id == group_id,
            )
            .first()
        )

        if group is None:
            raise NoSuchGroupException(f"{user_a},{user_b}")

        return GroupBase(**group.__dict__)

    async def create_group_for_1to1(self, user_a: int, user_b: int, db: AsyncSession) -> GroupBase:
        users = sorted([user_a, user_b])
        group_name = ",".join([str(user_id) for user_id in users])
        now = utcnow_dt()

        query = CreateGroupQuery(
            group_name=group_name, group_type=GroupTypes.ONE_TO_ONE, users=users
        )

        return await self.create_group(user_a, query, now, db)

    async def get_user_ids_and_join_time_in_groups(self, group_ids: List[str], db: AsyncSession) -> dict:
        group_and_users: Dict[str, Dict[int, float]] = \
            await self.env.cache.get_user_ids_and_join_time_in_groups(group_ids)

        if len(group_and_users) == len(group_ids):
            return group_and_users

        remaining_group_ids = [
            group_id
            for group_id in group_ids
            if group_id not in group_and_users.keys()
        ]

        users = await db.run_sync(lambda _db:
            _db.query(
                UserGroupStatsEntity.group_id,
                UserGroupStatsEntity.user_id,
                UserGroupStatsEntity.join_time,
            )
            .filter(
                UserGroupStatsEntity.group_id.in_(remaining_group_ids),
                UserGroupStatsEntity.kicked.is_(False)
            )
            .all()
        )

        if users is None or len(users) == 0:
            return group_and_users

        for group_id, user_id, join_time in users:
            if group_id not in group_and_users:
                group_and_users[group_id] = dict()
            group_and_users[group_id][user_id] = to_ts(join_time)

        await self.env.cache.set_user_ids_and_join_time_in_groups(group_and_users)
        return group_and_users

    async def get_user_ids_and_join_time_in_group(self, group_id: str, db: AsyncSession) -> dict:
        users = await self.env.cache.get_user_ids_and_join_time_in_group(group_id)

        if users is not None:
            return users

        users = await db.run_sync(lambda _db:
            _db.query(
                UserGroupStatsEntity.user_id,
                UserGroupStatsEntity.join_time,
            )
            .filter(
                UserGroupStatsEntity.group_id == group_id,
                UserGroupStatsEntity.kicked.is_(False)
            )
            .all()
        )

        if users is None or len(users) == 0:
            return dict()

        user_ids_join_time = {user[0]: to_ts(user[1]) for user in users}
        await self.env.cache.set_user_ids_and_join_time_in_group(group_id, user_ids_join_time)

        return user_ids_join_time

    # noinspection PyMethodMayBeStatic
    async def group_exists(self, group_id: str, db: AsyncSession) -> bool:
        group = await db.run_sync(lambda _db:
            _db.query(literal(True))
            .filter(GroupEntity.group_id == group_id)
            .first()
        )

        return group is not None

    # noinspection PyMethodMayBeStatic
    async def set_groups_updated_at(self, group_ids: List[str], now: dt, db: AsyncSession) -> None:
        await db.run_sync(lambda _db:
            _db.query(GroupEntity)
            .filter(GroupEntity.group_id.in_(group_ids))
            .update(
                {GroupEntity.updated_at: now},
                synchronize_session='fetch'
            )
        )
        await db.commit()

    async def update_user_stats_on_join_or_create_group(
        self, group_id: str, users: Dict[int, float], now: dt, group_type: int, db: AsyncSession
    ) -> None:
        user_ids_for_cache = set()
        user_ids_to_stats = dict()
        user_ids = list(users.keys())

        user_stats = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(UserGroupStatsEntity.user_id.in_(user_ids))
            .filter(UserGroupStatsEntity.group_id == group_id)
            .all()
        )

        for user_stat in user_stats:
            user_ids_to_stats[user_stat.user_id] = user_stat

        for user_id in user_ids:
            if user_id not in user_ids_to_stats:
                await self.env.cache.increase_count_group_types_for_user(user_id, GroupTypes.PRIVATE_GROUP)

                user_ids_for_cache.add(user_id)
                user_ids_to_stats[user_id] = await self._create_user_stats(
                    group_id=group_id,
                    user_id=user_id,
                    default_dt=now,
                    group_type=group_type
                )

            # reset the kicked variable when joining a group
            if user_ids_to_stats[user_id].kicked:
                user_ids_to_stats[user_id].kicked = False

            user_ids_to_stats[user_id].last_read = now
            db.add(user_ids_to_stats[user_id])

        if not len(user_ids_to_stats):
            return

        await db.commit()

        now_ts = to_ts(now)

        join_times = {
            # actual query happens, similar github issue: https://github.com/sqlalchemy/sqlalchemy/discussions/8913
            user_id: to_ts(await db.run_sync(lambda _db: stats.join_time))
            for user_id, stats in user_ids_to_stats.items()
        }
        read_times = {user_id: now_ts for user_id in user_ids}

        await self.env.cache.add_user_ids_and_join_time_in_group(group_id, join_times)
        await self.env.cache.set_last_read_in_group_for_users(group_id, read_times)

    async def count_group_types_for_user(
            self, user_id: int, query: GroupQuery, db: AsyncSession
    ) -> List[Tuple[int, int]]:
        hidden = query.hidden
        deleted = query.deleted
        types = list()

        # don't check cache for deleted, only supporters and admins use it
        if deleted is False:
            types = await self.env.cache.get_count_group_types_for_user(user_id, hidden)
            if types is not None:
                return types

        statement = await db.run_sync(lambda _db:
            _db.query(
                GroupEntity.group_type,
                func.count(GroupEntity.group_type)
            )
            .join(
                UserGroupStatsEntity,
                UserGroupStatsEntity.group_id == GroupEntity.group_id
            )
            .filter(
                UserGroupStatsEntity.user_id == user_id,
            )
        )

        # "include deleted? yes/no/both" based on the query.deleted parameter
        statement = update_statement_for_deleted_flag(statement, query.deleted)

        if hidden is not None:
            statement = statement.filter(
                UserGroupStatsEntity.hide.is_(hidden)
            )

        types = await db.run_sync(lambda _db:
            statement.group_by(
                GroupEntity.group_type
            )
            .all()
        )

        types_dict = defaultdict(lambda: 0)
        for the_type, the_count in types:
            types_dict[the_type] = the_count

        # make sure we have the cached amount for all possible group
        # types even if the user is not part of all group types
        for group_type in {GroupTypes.PRIVATE_GROUP, GroupTypes.ONE_TO_ONE, GroupTypes.PUBLIC_ROOM}:
            if group_type not in types_dict:
                types_dict[group_type] = 0

        types = list(types_dict.items())

        # don't cache if we're checking deleted groups
        # if hidden is None, we're counting for both types
        if deleted is False and hidden is not None:
            await self.env.cache.set_count_group_types_for_user(user_id, types, hidden)

        return types

    # noinspection PyMethodMayBeStatic
    async def set_last_updated_at_on_all_stats_related_to_user(self, user_id: int, db: AsyncSession):
        now = utcnow_dt()

        group_ids = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity.group_id)
            .filter(
                UserGroupStatsEntity.user_id == user_id
            )
            .all()
        )

        # query returns a single-item tuple of each group_id
        if len(group_ids):
            group_ids = [gid[0] for gid in group_ids]

        before = None
        if len(group_ids) > 250:
            before = utcnow_ts()

        # some users have >10k conversations; split into chunks to not overload the db
        for group_id_chunk in split_into_chunks(group_ids, 500):
            await db.run_sync(lambda _db:
                _db.query(UserGroupStatsEntity)
                .filter(
                    UserGroupStatsEntity.group_id.in_(group_id_chunk)
                )
                .update(
                    {UserGroupStatsEntity.last_updated_time: now},
                    synchronize_session="fetch",
                )
            )

        await db.commit()

        if before is not None:
            the_time = utcnow_ts() - before
            the_time = "%.2f" % the_time

            logger.info(f"updating {len(group_ids)} user group stats took {the_time}s")

    # noinspection PyMethodMayBeStatic
    async def set_last_updated_at_for_all_in_group(self, group_id: str, db: AsyncSession):
        now = utcnow_dt()

        await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(UserGroupStatsEntity.group_id == group_id)
            .update({UserGroupStatsEntity.last_updated_time: now})
        )

        await db.commit()

    # noinspection PyMethodMayBeStatic
    async def update_group_information(
        self, group_id: str, query: UpdateGroupQuery, db: AsyncSession
    ) -> Optional[GroupBase]:
        group_entity = await db.run_sync(lambda _db:
            _db.query(GroupEntity)
            .filter(GroupEntity.group_id == group_id)
            .first()
        )

        if group_entity is None:
            return None

        now = utcnow_dt()

        # default, frozen, archived, deleted
        if query.status is not None:
            group_entity.status_changed_at = now
            group_entity.status = query.status
            await self.env.cache.set_group_status(group_id, query.status)

        if query.group_name is not None:
            group_entity.name = query.group_name

        if query.description is not None:
            group_entity.description = query.description

        if query.owner is not None:
            group_entity.owner_id = query.owner

        group_entity.updated_at = now

        base = GroupBase(**group_entity.__dict__)

        db.add(group_entity)
        await db.commit()

        # need to query users after committing the previous transaction
        if query.status is not None and query.status == GroupStatus.DELETED:
            user_ids = await db.run_sync(lambda _db:
                _db.query(UserGroupStatsEntity.user_id)
                .filter(UserGroupStatsEntity.group_id == group_id)
                .all()
            )
            user_ids = [user_id[0] for user_id in user_ids]
            # actual query happens, similar github issue: https://github.com/sqlalchemy/sqlalchemy/discussions/8913
            group_id_to_type = {group_id: await db.run_sync(lambda _db: group_entity.group_type)}
            group_ids = [group_id]

            # deleting a group doesn't happen often, so it's okay we loop here
            for user_id in user_ids:
                await self.copy_to_deleted_groups_table(group_id_to_type, user_id, db, skip_public=False)
                await self.remove_user_group_stats_for_user(group_ids, user_id, db)
                await self.env.cache.reset_count_group_types_for_user(user_id)

        return base

    async def get_user_ids_in_groups(self, group_ids: List[str], db: AsyncSession) -> Dict[str, List[int]]:
        group_user_join_time = await self.get_user_ids_and_join_time_in_groups(group_ids, db)
        group_to_users = dict()

        for group_id, user_join_time in group_user_join_time.items():
            group_to_users[group_id] = list(user_join_time.keys())

        return group_to_users

    async def mark_all_groups_as_read(self, user_id: int, db: AsyncSession) -> List[str]:
        group_ids = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity.group_id)
            .join(
                GroupEntity,
                GroupEntity.group_id == UserGroupStatsEntity.group_id
            )
            .filter(
                UserGroupStatsEntity.user_id == user_id,
                or_(
                    UserGroupStatsEntity.last_read < GroupEntity.last_message_time,
                    UserGroupStatsEntity.bookmark.is_(True),
                    UserGroupStatsEntity.unread_count > 0,
                    UserGroupStatsEntity.mentions > 0
                )
            )
            .all()
        )

        await self.env.cache.reset_total_unread_message_count(user_id)

        # sqlalchemy returns a list of tuples: [(group_id1,), (group_id2,), ...]
        group_ids = [group_id[0] for group_id in group_ids]

        now = utcnow_dt()

        # some users have >10k conversations; split into chunks to not overload the db
        for group_id_chunk in split_into_chunks(group_ids, 500):
            await self.env.cache.reset_unread_in_groups(user_id, group_id_chunk)
            await self.env.cache.set_last_read_in_groups_for_user(group_ids, user_id, to_ts(now))

            await db.run_sync(lambda _db:
                _db.query(UserGroupStatsEntity)
                .filter(
                    UserGroupStatsEntity.group_id.in_(group_id_chunk),
                    UserGroupStatsEntity.user_id == user_id
                )
                .update(
                    {
                        UserGroupStatsEntity.last_updated_time: now,
                        UserGroupStatsEntity.last_read: now,
                        UserGroupStatsEntity.unread_count: 0,
                        UserGroupStatsEntity.mentions: 0,
                        UserGroupStatsEntity.bookmark: False,
                        UserGroupStatsEntity.highlight_time: self.long_ago
                    },
                    synchronize_session="fetch",
                )
            )

            # need to reset the highlight time on the other user's stats too
            await db.run_sync(lambda _db:
                _db.query(UserGroupStatsEntity)
                .filter(
                    UserGroupStatsEntity.group_id.in_(group_id_chunk),
                    UserGroupStatsEntity.user_id != user_id,
                    UserGroupStatsEntity.receiver_highlight_time > self.long_ago
                )
                .update(
                    {
                        UserGroupStatsEntity.receiver_highlight_time: self.long_ago
                    },
                    synchronize_session="fetch",
                )
            )

        await db.commit()

        return group_ids

    # noinspection PyMethodMayBeStatic
    async def get_all_user_stats_in_group(
            self, group_id: str, db: AsyncSession, include_kicked: bool = True
    ) -> List[UserGroupStatsBase]:
        statement = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(UserGroupStatsEntity.group_id == group_id)
        )

        if not include_kicked:
            statement = statement.filter(
                UserGroupStatsEntity.kicked.is_(False)
            )

        user_stats = await db.run_sync(lambda _db: statement.all())

        if user_stats is None:
            raise NoSuchGroupException(f"no users in group {group_id} (include_kicked? {include_kicked})")

        return [
            UserGroupStatsBase(**user_stat.__dict__)
            for user_stat in user_stats
        ]

    async def get_sent_message_count(self, group_id: str, user_id: int, db: AsyncSession) -> Optional[int]:
        sent = await self.env.cache.get_sent_message_count_in_group_for_user(group_id, user_id)
        if sent is not None:
            return sent

        sent = (await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity.sent_message_count)
            .filter(UserGroupStatsEntity.group_id == group_id)
            .filter(UserGroupStatsEntity.user_id == user_id)
            .first()
        ))[0]

        if sent == -1:
            return None

        await self.env.cache.set_sent_message_count_in_group_for_user(group_id, user_id, sent)
        return sent

    async def set_sent_message_count(self, group_id, user_id, message_count, db) -> None:
        await self.env.cache.set_sent_message_count_in_group_for_user(group_id, user_id, message_count)

        await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(UserGroupStatsEntity.group_id == group_id)
            .filter(UserGroupStatsEntity.user_id == user_id)
            .update({UserGroupStatsEntity.sent_message_count: message_count})
        )

        await db.commit()

    async def get_group_status(self, group_id: str, db: AsyncSession) -> Optional[int]:
        group_status = await self.env.cache.get_group_status(group_id)
        if group_status is not None:
            return group_status

        group = await db.run_sync(lambda _db:
            _db.query(GroupEntity)
            .filter(GroupEntity.group_id == group_id)
            .first()
        )

        # group doesn't exist (yet)
        if group is None:
            return None

        status = group.status
        if status is None:
            status = 0

        await self.env.cache.set_group_status(group_id, status)
        return group.status

    # noinspection PyMethodMayBeStatic
    async def get_user_stats_in_group(
        self, group_id: str, user_id: int, db: AsyncSession
    ) -> Optional[UserGroupStatsBase]:
        user_stats = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(UserGroupStatsEntity.user_id == user_id)
            .filter(UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        if user_stats is None:
            raise UserNotInGroupException(f"user {user_id} is not in group {group_id}")

        return UserGroupStatsBase(**user_stats.__dict__)

    async def get_both_user_stats_in_group(
            self,
            group_id: str,
            user_id: int,
            query: UpdateUserGroupStats,
            db: AsyncSession
    ) -> Tuple[UserGroupStatsEntity, Optional[UserGroupStatsEntity], Optional[GroupEntity]]:
        # when removing a bookmark, the highlight time will be reset
        # and unread count will become 0
        need_second_user_stats = (
            query.highlight_time is not None or
            query.last_read_time is not None or
            query.bookmark is False
        )

        statement = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(UserGroupStatsEntity.group_id == group_id)
        )

        that_user_stats = None
        group = None

        async def filter_for_one():
            return await db.run_sync(
                lambda _db: statement.filter(UserGroupStatsEntity.user_id == user_id).first()
            )

        if need_second_user_stats:
            group = await db.run_sync(lambda _db:
                _db.query(GroupEntity)
                .filter(GroupEntity.group_id == group_id)
                .first()
            )

            if group is None:
                raise NoSuchGroupException(group_id)

            if group.group_type == GroupTypes.ONE_TO_ONE:
                user_stats_dict = {user.user_id: user for user in await db.run_sync(lambda _db: statement.all())}
                that_user_id = [uid for uid in group_id_to_users(group.group_id) if uid != user_id][0]

                if user_id in user_stats_dict:
                    user_stats = user_stats_dict[user_id]
                else:
                    logger.warning(f"THIS user {user_id} is no longer in group {group_id}, ignoring stats")
                    user_stats = None

                if that_user_id in user_stats_dict:
                    that_user_stats = user_stats_dict[that_user_id]
                else:
                    logger.warning(f"THAT user {that_user_id} is no longer in group {group_id}, ignoring stats")
            else:
                user_stats = await filter_for_one()
        else:
            user_stats = await filter_for_one()

        return user_stats, that_user_stats, group

    async def update_user_group_stats(
        self, group_id: str, user_id: int, query: UpdateUserGroupStats, db: AsyncSession
    ) -> None:
        # delegate to separate handler, too much business logic
        await self.stats_handler.update(group_id, user_id, query, db)

    async def get_delete_before(self, group_id: str, user_id: int, db: AsyncSession) -> dt:
        delete_before = await self.env.cache.get_delete_before(group_id, user_id)
        if delete_before is not None:
            return delete_before

        delete_before = await db.run_sync(lambda _db:
            _db.query(
                UserGroupStatsEntity.delete_before
            )
            .filter(
                UserGroupStatsEntity.group_id == group_id,
                UserGroupStatsEntity.user_id == user_id
            )
            .first()
        )

        if delete_before is None or len(delete_before) == 0:
            raise NoSuchUserException(user_id)

        delete_before = delete_before[0]
        await self.env.cache.set_delete_before(group_id, user_id, to_ts(delete_before))

        return delete_before

    async def get_last_message_time_in_group(self, group_id: str, db: AsyncSession) -> dt:
        last_message_time = await self.env.cache.get_last_message_time_in_group(group_id)
        if last_message_time is not None:
            return to_dt(last_message_time)

        last_message_time = await db.run_sync(lambda _db:
            _db.query(
                GroupEntity.last_message_time
            )
            .filter(
                GroupEntity.group_id == group_id
            )
            .first()
        )

        if last_message_time is None or len(last_message_time) == 0:
            raise NoSuchGroupException(group_id)

        last_message_time = last_message_time[0]
        await self.env.cache.set_last_message_time_in_group(group_id, to_ts(last_message_time))

        return last_message_time

    async def update_last_read_and_highlight_in_group_for_user(
        self, group_id: str, user_id: int, the_time: dt, db: AsyncSession
    ) -> None:
        user_stats = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(UserGroupStatsEntity.user_id == user_id)
            .filter(UserGroupStatsEntity.group_id == group_id)
            .first()
        )
        if user_stats is None:
            raise UserNotInGroupException(f"user {user_id} is not in group {group_id}")

        current_highlight_time = to_ts(user_stats.highlight_time)
        long_ago_ts = to_ts(self.long_ago)

        user_stats.last_read = the_time
        user_stats.last_updated_time = the_time
        user_stats.highlight_time = self.long_ago
        user_stats.receiver_highlight_time = self.long_ago
        user_stats.bookmark = False
        user_stats.hide = False
        user_stats.mentions = 0
        user_stats.unread_count = 0

        # TODO: use pipeline
        # re-check next time from db and cache it
        await self.env.cache.remove_last_read_in_group_oldest(group_id)

        # /groups api will check the cache, need to update this value if we read a group
        await self.env.cache.set_unread_in_group(group_id, user_id, 0)

        # have to reset the highlight time (if any) of the other users in the group as well
        if current_highlight_time > long_ago_ts:
            # TODO: use update() instead of running multiple queries (select and update)
            other_user_stats = await db.run_sync(lambda _db:
                _db.query(UserGroupStatsEntity)
                .filter(UserGroupStatsEntity.user_id != user_id)
                .filter(UserGroupStatsEntity.group_id == group_id)
                .filter(UserGroupStatsEntity.receiver_highlight_time > self.long_ago)
                .all()
            )

            for other_stat in other_user_stats:
                other_stat.highlight_time = self.long_ago
                other_stat.receiver_highlight_time = self.long_ago
                db.add(other_stat)

        await self.env.cache.set_last_read_in_group_for_user(group_id, user_id, to_ts(the_time))

        db.add(user_stats)
        await db.commit()

    async def update_last_read_and_sent_in_group_for_user(
        self, group_id: str, user_id: int, the_time: dt, db: AsyncSession, unhide_group: bool = True
    ) -> None:
        """
        :param unhide_group: when creating action logs in all of a user's groups, e.g. when the user is changing
         username, an action log is created in all groups, but we don't want to unhide all groups
        """
        user_stats = await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(UserGroupStatsEntity.user_id == user_id)
            .filter(UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        the_time_ts = to_ts(the_time)
        current_unread_count = await self.env.cache.get_unread_in_group(group_id, user_id)

        # use a pipeline to connect the different redis calls
        async with self.env.cache.pipeline() as p:
            await self.env.cache.set_last_read_in_group_for_user(group_id, user_id, the_time_ts, pipeline=p)

            # used for user global stats api
            await self.env.cache.set_last_sent_for_user(user_id, group_id, the_time_ts, pipeline=p)
            await self.env.cache.set_unread_in_group(group_id, user_id, 0, pipeline=p)

            if unhide_group:
                await self.env.cache.set_hide_group(group_id, False, pipeline=p)

            # if the user sends a message while having unread messages in the group (maybe can happen on the app?)
            if current_unread_count is not None and current_unread_count > 0:
                await self.env.cache.reset_total_unread_message_count(user_id)

        if user_stats is None:
            raise UserNotInGroupException(f"user {user_id} is not in group {group_id}")

        user_stats.last_read = the_time
        user_stats.last_sent = the_time
        user_stats.last_sent_group_id = group_id
        user_stats.last_updated_time = the_time

        # if the user is sending a message without opening the conversation, we have to make sure the group
        # is not deleted or hidden after (opening a conversation would set these two to False as well)
        user_stats.deleted = False

        if unhide_group:
            user_stats.hide = False

        if user_stats.first_sent is None:
            user_stats.first_sent = the_time

        db.add(user_stats)
        await db.commit()

    async def create_group(
        self, owner_id: int, query: CreateGroupQuery, utc_now, db: AsyncSession
    ) -> GroupBase:
        # can't be exactly the same, because when listing groups for a
        # user, any group with only one message would not be included,
        # since we're doing a filter on delete_before < last_message_time,
        # and when joining a group (happens on first message) we set
        # "delete_before = join_time = created_at"; if we set
        # last_message_time to this time as well the filter won't include
        # the group
        created_at = trim_micros(arrow.get(utc_now).shift(seconds=-1).datetime)

        if query.group_type == GroupTypes.ONE_TO_ONE:
            group_id = users_to_group_id(*query.users)
        else:
            group_id = str(uuid())

        # cache the existence of the group before creating it, to try to avoid race
        # conditions when sending multiple fist messages at the same time
        await self.env.cache.set_group_exists(group_id, True)

        language = None
        if query.group_type in GroupTypes.public_group_types:
            await self.env.cache.add_public_group_ids([group_id])

            # only public groups can be for a specific language
            if query.language is not None and len(query.language) == 2:
                language = query.language

        group_entity = GroupEntity(
            group_id=group_id,
            name=query.group_name,
            group_type=query.group_type,
            last_message_time=utc_now,
            first_message_time=utc_now,
            updated_at=created_at,
            created_at=created_at,
            owner_id=owner_id,
            meta=query.meta,
            description=query.description,
            language=language
        )

        user_ids = {owner_id}
        user_ids.update(query.users)

        # TODO: otherwise, newly created groups won't show in `/groups` api;
        #  will groups ever be created without sending a message to them?
        delete_before = created_at - datetime.timedelta(seconds=1)

        for user_id in user_ids:
            await self.env.cache.increase_count_group_types_for_user(user_id, query.group_type)
            user_stats = await self._create_user_stats(
                group_id=group_id,
                user_id=user_id,
                group_type=query.group_type,
                default_dt=created_at,
                delete_before=delete_before
            )
            db.add(user_stats)

        base = GroupBase(**group_entity.__dict__)

        db.add(group_entity)

        try:
            await db.commit()
        except IntegrityError:
            try:
                # have to manually roll back this transaction, since we'll keep
                # using the session after this method returns
                await db.rollback()
            except Exception as e:
                logger.error(f"could not rollback: {str(e)}")
                logger.exception(e)

            # can happen when multiple messages are sent simultaneously as a first
            # contact, e.g. if the first contact is 9 images, one of those api calls
            # maybe have created the group/stats first, but when the other calls
            # checked, it hadn't yet been saved; in this case, the calling method has
            # to catch this exception and ignore it
            raise UserStatsOrGroupAlreadyCreated(
                "user stats or group exists, multiple api calls tried to create them at the same time"
            )

        return base

    # noinspection PyMethodMayBeStatic
    async def update_first_message_time(self, group_id: str, first_message_time: dt, db: AsyncSession) -> None:
        logger.info(f"group {group_id}: setting first_message_time = {first_message_time}")

        await db.run_sync(lambda _db:
            _db.query(GroupEntity)
            .filter(GroupEntity.group_id == group_id)
            .update({GroupEntity.first_message_time: first_message_time})
        )

        await db.commit()

    @time_coroutine(logger, "get_groups_with_undeleted_messages()")
    async def get_groups_with_undeleted_messages(self, db: AsyncSession):
        """
        Used for removing old messages from the system. It queries for the time
        of which every previous message for a group can be removed. Messages are
        to be removed when every user has a `delete_before` older than
        `first_message_time`. After removal, the new oldest message will be set
        as the value of `first_message_time`.

        What we want to do:

            select
                g.group_id,
                min(u.delete_before)
            from
                groups g,
                user_group_stats u
            where
                g.group_id = u.group_id
            group by
                g.group_id
            having
                coalesce(
                    sum(
                        case when u.delete_before <= g.first_message_time then 1
                        else 0 end
                    ),
                0) = 0;
        """
        return await db.run_sync(lambda _db:
            _db.query(
                GroupEntity.group_id,
                func.min(UserGroupStatsEntity.delete_before)
            )
            .join(
                UserGroupStatsEntity,
                UserGroupStatsEntity.group_id == GroupEntity.group_id
            )
            .group_by(
                GroupEntity.group_id
            )
            .having(
                func.coalesce(
                    func.sum(case(
                        [(UserGroupStatsEntity.delete_before <= GroupEntity.first_message_time, 1)],
                        else_=0
                    )),
                    0
                ) == 0
            )
            .all()
        )

    # noinspection PyMethodMayBeStatic
    async def _get_user_stats_for(self, group_id: str, user_id: int, db: AsyncSession):
        return await db.run_sync(lambda _db:
            _db.query(UserGroupStatsEntity)
            .filter(
                UserGroupStatsEntity.group_id == group_id,
                UserGroupStatsEntity.user_id == user_id,
            )
            .first()
        )

    async def _create_user_stats(
        self, group_id: str, user_id: int, default_dt: dt, group_type: int, delete_before: dt = None
    ) -> UserGroupStatsEntity:
        now = utcnow_dt()

        max_days = self.room_max_history_days
        max_count = self.room_max_history_count

        if group_type in GroupTypes.public_group_types:
            a_month_ago = arrow.get(now).shift(days=-max_days).datetime
            msg_count = await self.env.storage.count_messages_in_group_since(group_id, a_month_ago)

            # max 500 messages or 1 month ago
            if msg_count > max_count:
                delete_before = await self.env.storage.get_created_at_for_offset(group_id, offset=max_count - 1)
                if delete_before is None:
                    logger.error(f"no messages in group '{group_id}', but count > {max_count}")
                    delete_before = a_month_ago
            else:
                delete_before = a_month_ago

        elif delete_before is None:
            delete_before = default_dt

        return UserGroupStatsEntity(
            group_id=group_id,
            user_id=user_id,
            last_read=default_dt,
            delete_before=delete_before,
            last_sent=default_dt,
            join_time=default_dt,
            last_updated_time=now,
            hide=False,
            pin=False,
            deleted=False,
            highlight_time=self.long_ago,
            receiver_highlight_time=self.long_ago,
            # for new groups, we can set this to 0 directly and start counting, instead of the default -1
            sent_message_count=0,
            mentions=0
        )
