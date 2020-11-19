import logging
from datetime import datetime as dt
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from uuid import uuid4 as uuid

import arrow
from sqlalchemy import case
from sqlalchemy import func
from sqlalchemy import literal
from sqlalchemy import or_
from sqlalchemy.orm import Session

from dinofw.db.rdbms import models
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.models import AbstractQuery
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import GroupUpdatesQuery
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UpdateUserGroupStats
from dinofw.utils import split_into_chunks
from dinofw.utils import trim_micros
from dinofw.utils import utcnow_dt
from dinofw.utils import utcnow_ts
from dinofw.utils.config import GroupTypes
from dinofw.utils.decorators import time_method
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import UserNotInGroupException

logger = logging.getLogger(__name__)


def users_to_group_id(user_a: int, user_b: int) -> str:
    # convert integer ids to hex; need to be sorted
    users = map(hex, sorted([user_a, user_b]))

    # drop the initial '0x' and left-pad with zeros (a uuid is two
    # 16 character parts, so pad to length 16)
    u = "".join([user[2:].zfill(16) for user in users])

    # insert dashes at the correct places
    return f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"


def group_id_to_users(group_id: str) -> (int, int):
    group_id = group_id.replace("-", "")
    user_a = int(group_id[:16].lstrip("0"), 16)
    user_b = int(group_id[16:].lstrip("0"), 16)

    return sorted([user_a, user_b])


class RelationalHandler:
    def __init__(self, env):
        self.env = env

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = dt.utcfromtimestamp(beginning_of_1995)

    def get_users_in_group(
        self, group_id: str, db: Session
    ) -> (Optional[GroupBase], Optional[Dict[int, float]], Optional[int]):
        group_entity = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        if group_entity is None:
            raise NoSuchGroupException(group_id)

        group = GroupBase(**group_entity.__dict__)
        users_and_join_time = self.get_user_ids_and_join_time_in_group(group_id, db)
        user_count = len(users_and_join_time)

        return group, users_and_join_time, user_count

    def get_last_sent_for_user(self, user_id: int, db: Session) -> (str, float):
        group_id, last_sent = self.env.cache.get_last_sent_for_user(user_id)
        if group_id is not None:
            return group_id, last_sent

        group_id_and_last_sent = (
            db.query(
                models.UserGroupStatsEntity.group_id,
                models.UserGroupStatsEntity.last_sent
            )
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .order_by(models.UserGroupStatsEntity.last_sent)
            .limit(1)
            .first()
        )

        if group_id_and_last_sent is None:
            return None, None

        group_id, last_sent = group_id_and_last_sent
        last_sent = AbstractQuery.to_ts(last_sent)
        self.env.cache.set_last_sent_for_user(user_id, group_id, last_sent)

        return group_id, last_sent

    # noinspection PyMethodMayBeStatic
    def get_group_ids_and_created_at_for_user(self, user_id: int, db: Session) -> List[Tuple[str, dt]]:
        groups = (
            db.query(
                models.GroupEntity.group_id,
                models.GroupEntity.created_at,
            )
            .join(
                models.UserGroupStatsEntity,
                models.UserGroupStatsEntity.group_id == models.GroupEntity.group_id,
            )
            .filter(
                models.UserGroupStatsEntity.user_id == user_id
            )
            .all()
        )

        return groups

    def get_groups_for_user(
        self,
        user_id: int,
        query: GroupQuery,
        db: Session,
        count_receiver_unread: bool = True,
        receiver_stats: bool = False,
    ) -> List[UserGroupBase]:
        """
        what we're doing:

        select * from
            groups g
        inner join
            user_group_stats u on u.group_id = g.group_id
        where
            u.user_id = 1234 and
            u.hide = false and
            u.delete_before < g.last_message_time and
            g.last_message_time <= until
        order by
            u.pin desc,
            greatest(u.highlight_time, g.last_message_time) desc
        limit per_page
        """
        @time_method(logger, "get_groups_for_user(): query groups")
        def query_groups():
            until = GroupQuery.to_dt(query.until)

            statement = (
                db.query(
                    models.GroupEntity,
                    models.UserGroupStatsEntity
                )
                .join(
                    models.UserGroupStatsEntity,
                    models.UserGroupStatsEntity.group_id == models.GroupEntity.group_id
                )
                .filter(
                    models.GroupEntity.last_message_time < until,
                    models.UserGroupStatsEntity.delete_before <= models.GroupEntity.updated_at,
                    # TODO: when joining a "group", the last message was before you joined; if we create
                    #  an action log when a user joins it will update `last_message_time` and we can use
                    #  that instead of `updated_at`, which would make more sense
                    # models.UserGroupStatsEntity.delete_before < models.GroupEntity.last_message_time,
                    models.UserGroupStatsEntity.user_id == user_id,
                )
            )

            if query.hidden is not None:
                statement = statement.filter(
                    models.UserGroupStatsEntity.hide.is_(query.hidden),
                )

            if query.only_unread:
                statement = statement.filter(
                    or_(
                        models.UserGroupStatsEntity.last_read < models.GroupEntity.last_message_time,
                        models.UserGroupStatsEntity.bookmark.is_(True),
                    )
                )

            statement = (
                statement.order_by(
                    models.UserGroupStatsEntity.pin.desc(),
                    func.greatest(
                        models.UserGroupStatsEntity.highlight_time,
                        models.GroupEntity.last_message_time,
                    ).desc(),
                )
                .limit(query.per_page)
            )

            return statement.all()

        results = query_groups()
        receiver_stats_base = self.get_receiver_stats(results, user_id, receiver_stats, db)
        count_unread = query.count_unread or False

        return self.format_group_stats_and_count_unread(
            db,
            results,
            receiver_stats=receiver_stats_base,
            user_id=user_id,
            count_unread=count_unread,
            count_receiver=count_receiver_unread,  # when getting user stats we don't care about receivers
        )

    def get_groups_updated_since(
        self,
        user_id: int,
        query: GroupUpdatesQuery,
        db: Session,
        receiver_stats: bool = False,
    ):
        """
        the only difference between get_groups_for_user() and get_groups_updated_since() is
        that this method doesn't care about last_message_time, hide, delete_before, since
        this method is used to sync changed to different devices. This method is also
        filtering by "since" instead of "until", because for syncing we're paginating
        "forwards" instead of "backwards"
        """
        @time_method(logger, "get_groups_updated_since(): query groups")
        def query_groups():
            since = GroupUpdatesQuery.to_dt(query.since)

            return (
                db.query(models.GroupEntity, models.UserGroupStatsEntity)
                .filter(
                    models.GroupEntity.group_id == models.UserGroupStatsEntity.group_id,
                    models.UserGroupStatsEntity.user_id == user_id,
                    models.UserGroupStatsEntity.last_updated_time >= since,
                )
                .order_by(
                    models.UserGroupStatsEntity.pin.desc(),
                    func.greatest(
                        models.UserGroupStatsEntity.highlight_time,
                        models.GroupEntity.last_message_time,
                    ).desc(),
                )
                .limit(query.per_page)
                .all()
            )

        results = query_groups()
        receiver_stats = self.get_receiver_stats(results, user_id, receiver_stats, db)
        count_unread = query.count_unread or False

        return self.format_group_stats_and_count_unread(
            db, results, receiver_stats, user_id, count_unread
        )

    @time_method(logger, "get_receiver_stats()")
    def get_receiver_stats(self, results, user_id, receiver_stats, db: Session):
        if not receiver_stats:
            return list()

        group_ids = list()
        for group, stats in results:
            if group.group_type == GroupTypes.ONE_TO_ONE:
                group_ids.append(group.group_id)

        if len(group_ids):
            return self.get_receiver_user_stats(group_ids, user_id, db)

        return list()

    # noinspection PyMethodMayBeStatic
    def get_receiver_user_stats(self, group_ids: List[str], user_id: int, db: Session):
        return (
            db.query(models.UserGroupStatsEntity)
            .filter(
                models.UserGroupStatsEntity.group_id.in_(group_ids),
                models.UserGroupStatsEntity.user_id != user_id,
            )
            .all()
        )

    @time_method(logger, "format_group_stats_and_count_unread()")
    def format_group_stats_and_count_unread(
        self,
        db: Session,
        results: List[Tuple[models.GroupEntity, models.UserGroupStatsEntity]],
        receiver_stats: List[models.UserGroupStatsEntity],
        user_id: int,
        count_unread: bool,
        count_receiver: bool = True,
    ) -> List[UserGroupBase]:
        def count_for_group():
            _unread_count = -1
            _receiver_unread_count = -1

            if not count_unread:
                return _unread_count, _receiver_unread_count

            # only count for receiver if it's a 1v1 group
            if group.group_type == GroupTypes.ONE_TO_ONE and count_receiver:
                user_a, user_b = group_id_to_users(group.group_id)
                user_to_count_for = (
                    user_a if user_b == user_id else user_b
                )
                _receiver_unread_count = self.env.storage.get_unread_in_group(
                    group_id=group.group_id,
                    user_id=user_to_count_for,
                    last_read=user_group_stats.last_read,
                )

            _unread_count = self.env.storage.get_unread_in_group(
                group_id=group.group_id,
                user_id=user_id,
                last_read=user_group_stats.last_read,
            )

            return _unread_count, _receiver_unread_count

        groups = list()

        receivers = dict()
        for stat in receiver_stats:
            receivers[stat.group_id] = UserGroupStatsBase(**stat.__dict__)

        # batch all redis/db queries for join times
        group_users_join_time = self.get_user_ids_and_join_time_in_groups(
            [group.group_id for group, user_stats in results],
            db
        )

        for group_entity, user_group_stats_entity in results:
            group = GroupBase(**group_entity.__dict__)
            user_group_stats = UserGroupStatsBase(**user_group_stats_entity.__dict__)

            unread_count, receiver_unread_count = count_for_group()

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

    def update_group_new_message(
        self,
        message: MessageBase,
        sent_time: dt,
        db: Session,
        wakeup_users: bool = True,
    ) -> None:
        group = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == message.group_id)
            .first()
        )

        if group is None:
            raise NoSuchGroupException(message.group_id)

        # for knowing if we need to send read-receipts when user opens a conversation
        self.env.cache.set_last_message_time_in_group(
            message.group_id,
            AbstractQuery.to_ts(sent_time)
        )

        group.last_message_time = sent_time
        group.last_message_overview = message.message_payload
        group.last_message_id = message.message_id
        group.last_message_type = message.message_type
        group.last_message_user_id = message.user_id

        statement = (
            db.query(models.UserGroupStatsEntity)
            .filter(
                models.UserGroupStatsEntity.group_id == message.group_id
            )
        )

        # when creating action logs, we want to sync changes to apps, but not necessarily un-hide a group
        # TODO: maybe we actually want to wake them up, check with stakeholders
        if wakeup_users:
            statement.update({
                models.UserGroupStatsEntity.last_updated_time: sent_time,
                models.UserGroupStatsEntity.delete_before: models.UserGroupStatsEntity.join_time,
                models.UserGroupStatsEntity.hide: False,
            })
        else:
            statement.update({
                models.UserGroupStatsEntity.last_updated_time: sent_time,
            })

        db.add(group)
        db.commit()

    def get_last_reads_in_group(self, group_id: str, db: Session) -> Dict[int, float]:
        # TODO: rethink this; some cached some not? maybe we don't have to do this twice
        users = self.get_user_ids_and_join_time_in_group(group_id, db)
        user_ids = list(users.keys())
        return self.get_last_read_in_group_for_users(group_id, user_ids, db)

    @time_method(logger, "get_last_read_in_group_for_users()")
    def get_last_read_in_group_for_users(
        self, group_id: str, user_ids: List[int], db: Session
    ) -> Dict[int, float]:
        last_reads, not_cached = self.env.cache.get_last_read_in_group_for_users(
            group_id, user_ids
        )

        # got everything from the cache
        if not len(not_cached):
            return last_reads

        reads = (
            db.query(models.UserGroupStatsEntity)
            .with_entities(
                models.UserGroupStatsEntity.user_id,
                models.UserGroupStatsEntity.last_read,
            )
            .filter(
                models.UserGroupStatsEntity.group_id == group_id,
                models.UserGroupStatsEntity.user_id.in_(not_cached),
            )
            .all()
        )

        for user_id, last_read in reads:
            last_read_float = GroupQuery.to_ts(last_read)
            last_reads[user_id] = last_read_float

        self.env.cache.set_last_read_in_group_for_users(
            group_id, last_reads
        )

        return last_reads

    # noinspection PyMethodMayBeStatic
    def get_all_group_ids_for_user(self, user_id: int, db: Session) -> List[str]:
        """
        used only when a user is deleting their profile, no need
        to cache it, shouldn't happen that often
        """
        group_ids = (
            db.query(
                models.UserGroupStatsEntity.group_id
            )
            .filter(
                models.UserGroupStatsEntity.user_id == user_id
            )
            .all()
        )

        if group_ids is None or len(group_ids) == 0:
            return list()

        return [group_id[0] for group_id in group_ids]

    def get_group_id_for_1to1(
        self, user_a: int, user_b: int, db: Session
    ) -> Optional[str]:
        group = self.get_group_for_1to1(user_a, user_b, db)
        return group.group_id

    # noinspection PyMethodMayBeStatic
    def get_group_from_id(self, group_id: str, db: Session) -> GroupBase:
        group = (
            db.query(models.GroupEntity)
            .filter(
                models.GroupEntity.group_id == group_id,
            )
            .first()
        )

        if group is None:
            raise NoSuchGroupException(group_id)

        return GroupBase(**group.__dict__)

    # noinspection PyMethodMayBeStatic
    def get_group_for_1to1(
        self, user_a: int, user_b: int, db: Session, parse_result: bool = True
    ):
        group_id = users_to_group_id(user_a, user_b)

        group = (
            db.query(models.GroupEntity)
            .filter(
                models.GroupEntity.group_type == GroupTypes.ONE_TO_ONE,
                models.GroupEntity.group_id == group_id,
            )
            .first()
        )

        if group is None:
            raise NoSuchGroupException(f"{user_a},{user_b}")

        if parse_result:
            return GroupBase(**group.__dict__)

        return group_id

    def create_group_for_1to1(self, user_a: int, user_b: int, db: Session) -> GroupBase:
        users = sorted([user_a, user_b])
        group_name = ",".join([str(user_id) for user_id in users])

        query = CreateGroupQuery(
            group_name=group_name, group_type=GroupTypes.ONE_TO_ONE, users=users
        )

        return self.create_group(user_a, query, db)

    def get_user_ids_and_join_time_in_groups(self, group_ids: List[str], db: Session) -> dict:
        group_and_users: Dict[str, Dict[int, float]] = \
            self.env.cache.get_user_ids_and_join_time_in_groups(group_ids)

        if len(group_and_users) == len(group_ids):
            return group_and_users

        remaining_group_ids = [
            group_id
            for group_id in group_ids
            if group_id not in group_and_users.keys()
        ]

        users = (
            db.query(
                models.UserGroupStatsEntity.group_id,
                models.UserGroupStatsEntity.user_id,
                models.UserGroupStatsEntity.join_time,
            )
            .filter(models.UserGroupStatsEntity.group_id.in_(remaining_group_ids))
            .all()
        )

        if users is None or len(users) == 0:
            return group_and_users

        for group_id, user_id, join_time in users:
            if group_id not in group_and_users:
                group_and_users[group_id] = dict()
            group_and_users[group_id][user_id] = GroupQuery.to_ts(join_time)

        self.env.cache.set_user_ids_and_join_time_in_groups(group_and_users)
        return group_and_users

    def get_user_ids_and_join_time_in_group(self, group_id: str, db: Session) -> dict:
        users = self.env.cache.get_user_ids_and_join_time_in_group(group_id)

        if users is not None:
            return users

        users = (
            db.query(
                models.UserGroupStatsEntity.user_id,
                models.UserGroupStatsEntity.join_time,
            )
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .all()
        )

        if users is None or len(users) == 0:
            return dict()

        user_ids_join_time = {user[0]: GroupQuery.to_ts(user[1]) for user in users}
        self.env.cache.set_user_ids_and_join_time_in_group(group_id, user_ids_join_time)

        return user_ids_join_time

    def remove_last_read_in_group_for_user(
        self, group_id: str, user_id: int, db: Session
    ) -> None:
        """
        called when a user leaves a group
        """
        _ = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .delete()
        )
        db.commit()

        self.env.cache.remove_last_read_in_group_for_user(group_id, user_id)
        self.env.cache.clear_user_ids_and_join_time_in_group(group_id)

    # noinspection PyMethodMayBeStatic
    def group_exists(self, group_id: str, db: Session) -> bool:
        group = (
            db.query(literal(True))
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        return group is not None

    # noinspection PyMethodMayBeStatic
    def set_group_updated_at(self, group_id: str, now: dt, db: Session) -> None:
        group = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        if group is None:
            return

        group.updated_at = now

        db.add(group)
        db.commit()

    def update_user_stats_on_join_or_create_group(
        self, group_id: str, users: Dict[int, float], now: dt, db: Session
    ) -> None:
        user_ids_for_cache = set()
        user_ids_to_stats = dict()
        user_ids = list(users.keys())

        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id.in_(user_ids))
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .all()
        )

        for user_stat in user_stats:
            user_ids_to_stats[user_stat.user_id] = user_stat

        for user_id in user_ids:
            if user_id not in user_ids_to_stats:
                self.env.cache.reset_count_group_types_for_user(user_id)

                user_ids_for_cache.add(user_id)
                user_ids_to_stats[user_id] = self._create_user_stats(
                    group_id, user_id, now
                )

            user_ids_to_stats[user_id].last_read = now
            db.add(user_ids_to_stats[user_id])

        db.commit()
        now_ts = utcnow_ts()

        join_times = {
            user_id: GroupQuery.to_ts(stats.join_time)
            for user_id, stats in user_ids_to_stats.items()
        }
        read_times = {user_id: now_ts for user_id in user_ids}

        self.env.cache.add_user_ids_and_join_time_in_group(group_id, join_times)
        self.env.cache.set_last_read_in_group_for_users(group_id, read_times)

    def count_group_types_for_user(self, user_id: int, query: GroupQuery, db: Session) -> List[Tuple[int, int]]:
        hidden = query.hidden

        types = self.env.cache.get_count_group_types_for_user(user_id, hidden)
        if types is not None:
            return types

        statement = (
            db.query(
                models.GroupEntity.group_type,
                func.count(models.GroupEntity.group_type),
            )
            .join(
                models.UserGroupStatsEntity,
                models.UserGroupStatsEntity.group_id == models.GroupEntity.group_id,
            )
            .filter(
                models.UserGroupStatsEntity.user_id == user_id,
                models.UserGroupStatsEntity.delete_before < models.GroupEntity.last_message_time,
            )
        )

        if query.hidden is not None:
            statement = statement.filter(
                models.UserGroupStatsEntity.hide.is_(hidden)
            )

        types = (
            statement.group_by(
                models.GroupEntity.group_type
            )
            .limit(
                query.per_page
            )
            .all()
        )

        # if hidden is None, we're counting for both types
        if query.hidden is not None:
            self.env.cache.set_count_group_types_for_user(user_id, types, hidden)

        return types

    # noinspection PyMethodMayBeStatic
    def set_last_updated_at_on_all_stats_related_to_user(self, user_id: int, db: Session):
        now = utcnow_dt()

        group_ids = (
            db.query(models.UserGroupStatsEntity.group_id)
            .filter(
                models.UserGroupStatsEntity.user_id == user_id
            )
            .all()
        )

        before = None
        if len(group_ids) > 250:
            before = utcnow_ts()

        # some users have >10k conversations; split into chunks to not overload the db
        for group_id_chunk in split_into_chunks(group_ids, 500):
            _ = (
                db.query(models.UserGroupStatsEntity)
                .filter(
                    models.UserGroupStatsEntity.group_id.in_(group_id_chunk)
                )
                .update(
                    {models.UserGroupStatsEntity.last_updated_time: now},
                    synchronize_session="fetch",
                )
            )

        db.commit()

        if before is not None:
            the_time = utcnow_ts() - before
            the_time = "%.2f" % the_time

            logger.info(f"updating {len(group_ids)} user group stats took {the_time}s")

    # noinspection PyMethodMayBeStatic
    def set_last_updated_at_for_all_in_group(self, group_id: str, db: Session):
        now = utcnow_dt()

        _ = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .update({models.UserGroupStatsEntity.last_updated_time: now})
        )

        db.commit()

    # noinspection PyMethodMayBeStatic
    def update_group_information(
        self, group_id: str, query: UpdateGroupQuery, db: Session
    ) -> Optional[GroupBase]:
        group_entity = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        if group_entity is None:
            return None

        now = utcnow_dt()

        if query.name is not None:
            group_entity.name = query.name

        if query.context is not None:
            group_entity.group_context = query.context

        if query.owner is not None:
            group_entity.owner_id = query.owner

        group_entity.updated_at = now

        base = GroupBase(**group_entity.__dict__)

        db.add(group_entity)
        db.commit()

        return base

    def mark_all_groups_as_read(self, user_id: int, db: Session) -> None:
        group_ids = (
            db.query(models.UserGroupStatsEntity.group_id)
            .join(
                models.GroupEntity,
                models.GroupEntity.group_id == models.UserGroupStatsEntity.group_id
            )
            .filter(
                models.UserGroupStatsEntity.user_id == user_id,
                or_(
                    models.UserGroupStatsEntity.last_read < models.GroupEntity.last_message_time,
                    models.UserGroupStatsEntity.bookmark.is_(True),
                )
            )
            .all()
        )

        # sqlalchemy returns a list of tuples: [(group_id1,), (group_id2,), ...]
        group_ids = [group_id[0] for group_id in group_ids]

        now = utcnow_dt()

        # some users have >10k conversations; split into chunks to not overload the db
        for group_id_chunk in split_into_chunks(group_ids, 500):
            self.env.cache.reset_unread_in_groups(user_id, group_id_chunk)

            _ = (
                db.query(models.UserGroupStatsEntity)
                .filter(
                    models.UserGroupStatsEntity.group_id.in_(group_id_chunk)
                )
                .update(
                    {
                        models.UserGroupStatsEntity.last_updated_time: now,
                        models.UserGroupStatsEntity.last_read: now,
                        models.UserGroupStatsEntity.bookmark: False,
                    },
                    synchronize_session="fetch",
                )
            )

        db.commit()

    # noinspection PyMethodMayBeStatic
    def get_user_stats_in_group(
        self, group_id: str, user_id: int, db: Session
    ) -> Optional[UserGroupStatsBase]:
        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        if user_stats is None:
            raise UserNotInGroupException(f"user {user_id} is not in group {group_id}")

        base = UserGroupStatsBase(**user_stats.__dict__)

        return base

    def update_user_group_stats(
        self, group_id: str, user_id: int, query: UpdateUserGroupStats, db: Session
    ) -> None:
        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        last_read = UpdateUserGroupStats.to_dt(query.last_read_time, allow_none=True)
        delete_before = UpdateUserGroupStats.to_dt(query.delete_before, allow_none=True)
        highlight_time = UpdateUserGroupStats.to_dt(
            query.highlight_time, allow_none=True
        )
        now = utcnow_dt()

        if user_stats is None:
            raise UserNotInGroupException(
                f"tried to update group stats for user {user_id} not in group {group_id}"
            )

        # only update if query has new values
        else:
            # used by apps to sync changes
            user_stats.last_updated_time = now

            if query.bookmark is not None:
                user_stats.bookmark = query.bookmark

            if query.pin is not None:
                user_stats.pin = query.pin

            if query.rating is not None:
                user_stats.rating = query.rating

            if last_read is not None:
                user_stats.last_read = last_read

                # highlight time is removed if a user reads a conversation
                user_stats.highlight_time = self.long_ago

            if delete_before is not None:
                user_stats.delete_before = delete_before

            # can't set highlight time if also setting last read time
            if highlight_time is not None and last_read is None:
                user_stats.highlight_time = highlight_time

                # always becomes unhidden if highlighted
                user_stats.hide = False
                self.env.cache.set_hide_group(group_id, False, [user_id])

            elif query.hide is not None:
                user_stats.hide = query.hide
                self.env.cache.set_hide_group(group_id, query.hide, [user_id])

            if query.hide is not None or query.delete_before is not None:
                self.env.cache.reset_count_group_types_for_user(user_id)

        db.add(user_stats)
        db.commit()

    def get_last_message_time_in_group(self, group_id: str, db: Session) -> dt:
        last_message_time = self.env.cache.get_last_message_time_in_group(group_id)
        if last_message_time is not None:
            return AbstractQuery.to_dt(last_message_time)

        last_message_time = (
            db.query(
                models.GroupEntity.last_message_time
            )
            .filter(
                models.GroupEntity.group_id == group_id
            )
            .first()
        )

        if last_message_time is None or len(last_message_time) == 0:
            raise NoSuchGroupException(group_id)

        last_message_time = last_message_time[0]
        self.env.cache.set_last_message_time_in_group(group_id, AbstractQuery.to_ts(last_message_time))

        return last_message_time

    def update_last_read_and_highlight_in_group_for_user(
        self, group_id: str, user_id: int, the_time: dt, db: Session
    ) -> None:
        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        if user_stats is None:
            raise UserNotInGroupException(f"user {user_id} is not in group {group_id}")

        user_stats.last_read = the_time
        user_stats.last_updated_time = the_time
        user_stats.highlight_time = self.long_ago
        user_stats.bookmark = False

        db.add(user_stats)
        db.commit()

    def update_last_read_and_sent_in_group_for_user(
        self, group_id: str, user_id: int, the_time: dt, db: Session
    ) -> None:
        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        the_time_ts = GroupQuery.to_ts(the_time)
        self.env.cache.set_last_read_in_group_for_user(group_id, user_id, the_time_ts)

        # used for user global stats api
        self.env.cache.set_last_sent_for_user(user_id, group_id, the_time_ts)

        self.env.cache.set_hide_group(group_id, False)
        self.env.cache.set_unread_in_group(group_id, user_id, 0)

        if user_stats is None:
            raise UserNotInGroupException(f"user {user_id} is not in group {group_id}")

        user_stats.last_read = the_time
        user_stats.last_sent = the_time
        user_stats.last_sent_group_id = group_id
        user_stats.last_updated_time = the_time

        if user_stats.first_sent is None:
            user_stats.first_sent = the_time

        db.add(user_stats)
        db.commit()

    def create_group(
        self, owner_id: int, query: CreateGroupQuery, db: Session
    ) -> GroupBase:
        utc_now = arrow.utcnow()
        now = trim_micros(utc_now.datetime)

        # can't be exactly the same, because when listing groups for a
        # user, any group with only one message would not be included,
        # since we're doing a filter on delete_before < last_message_time,
        # and when joining a group (happens on first message) we set
        # "delete_before = join_time = created_at"; if we set
        # last_message_time to this time as well the filter won't include
        # the group
        created_at = trim_micros(utc_now.shift(seconds=-1).datetime)

        if query.group_type == GroupTypes.ONE_TO_ONE:
            group_id = users_to_group_id(*query.users)
        else:
            group_id = str(uuid())

        group_entity = models.GroupEntity(
            group_id=group_id,
            name=query.group_name,
            group_type=query.group_type,
            last_message_time=now,
            first_message_time=now,
            updated_at=created_at,
            created_at=created_at,
            owner_id=owner_id,
            meta=query.meta,
            context=query.context,
            description=query.description,
        )

        for user_id in query.users:
            self.env.cache.reset_count_group_types_for_user(user_id)
            user_stats = self._create_user_stats(
                group_entity.group_id, user_id, created_at
            )
            db.add(user_stats)

        base = GroupBase(**group_entity.__dict__)

        db.add(group_entity)
        db.commit()

        return base

    # noinspection PyMethodMayBeStatic
    def update_first_message_time(self, group_id: str, first_message_time: dt, db: Session) -> None:
        logger.info(f"group {group_id}: setting first_message_time = {first_message_time}")

        _ = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == group_id)
            .update({models.GroupEntity.first_message_time: first_message_time})
        )

        db.commit()

    @time_method(logger, "get_groups_with_undeleted_messages()")
    def get_groups_with_undeleted_messages(self, db: Session):
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
        return (
            db.query(
                models.GroupEntity.group_id,
                func.min(models.UserGroupStatsEntity.delete_before)
            )
            .join(
                models.UserGroupStatsEntity,
                models.UserGroupStatsEntity.group_id == models.GroupEntity.group_id
            )
            .group_by(
                models.GroupEntity.group_id
            )
            .having(
                func.coalesce(
                    func.sum(case(
                        [(models.UserGroupStatsEntity.delete_before <= models.GroupEntity.first_message_time, 1)],
                        else_=0
                    )),
                    0
                ) == 0
            )
            .all()
        )

    # noinspection PyMethodMayBeStatic
    def _get_user_stats_for(self, group_id: str, user_id: int, db: Session):
        return (
            db.query(models.UserGroupStatsEntity)
            .filter(
                models.UserGroupStatsEntity.group_id == group_id,
                models.UserGroupStatsEntity.user_id == user_id,
            )
            .first()
        )

    def _create_user_stats(
        self, group_id: str, user_id: int, default_dt: dt
    ) -> models.UserGroupStatsEntity:
        now = utcnow_dt()

        return models.UserGroupStatsEntity(
            group_id=group_id,
            user_id=user_id,
            last_read=default_dt,
            delete_before=default_dt,  # TODO: for group chats, should this be long_ago or join_time? to see old history
            last_sent=default_dt,
            join_time=default_dt,
            last_updated_time=now,
            hide=False,
            pin=False,
            highlight_time=self.long_ago,
        )
