import logging
from datetime import datetime as dt
from typing import List, Optional, Dict, Tuple, Any
from uuid import uuid4 as uuid

import arrow
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import literal
from sqlalchemy.orm import Session

from dinofw.db.rdbms import models
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.models import CreateGroupQuery, AbstractQuery
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import GroupUpdatesQuery
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UpdateUserGroupStats
from dinofw.utils.config import GroupTypes
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import UserNotInGroupException


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


def split_into_chunks(l, n):
    for i in range(0, len(l), n):
        # yields successive n-sized chunks of data
        yield l[i:i + n]


class RelationalHandler:
    def __init__(self, env):
        self.env = env

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = dt.utcfromtimestamp(beginning_of_1995)

        self.logger = logging.getLogger(__name__)

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
        until = GroupQuery.to_dt(query.until)
        hidden = query.hidden or False

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
                models.GroupEntity.last_message_time <= until,
                models.UserGroupStatsEntity.hide.is_(hidden),
                models.UserGroupStatsEntity.delete_before <= models.GroupEntity.updated_at,
                # TODO: when joining a "group", the last message was before you joined; if we create
                #  an action log when a user joins it will update `last_message_time` and we can use
                #  that instead of `updated_at`, which would make more sense
                # models.UserGroupStatsEntity.delete_before < models.GroupEntity.last_message_time,
                models.UserGroupStatsEntity.user_id == user_id,
            )
        )

        if query.only_unread:
            statement = statement.filter(
                or_(
                    models.UserGroupStatsEntity.last_read < models.GroupEntity.last_message_time,
                    models.UserGroupStatsEntity.bookmark.is_(True),
                )
            )

        results = (
            statement.order_by(
                models.UserGroupStatsEntity.pin.desc(),
                func.greatest(
                    models.UserGroupStatsEntity.highlight_time,
                    models.GroupEntity.last_message_time,
                ).desc(),
            )
            .limit(query.per_page)
            .all()
        )

        receiver_stats_base = list()
        if receiver_stats:
            group_ids = list()

            for group, stats in results:
                if group.group_type != GroupTypes.ONE_TO_ONE:
                    continue
                group_ids.append(group.group_id)

            receiver_stats_base = self.get_receiver_user_stats(group_ids, user_id, db)

        count_unread = query.count_unread or False
        return self._group_and_stats_to_user_group_base(
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
        since = GroupUpdatesQuery.to_dt(query.since)
        count_unread = query.count_unread or False

        results = (
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

        receiver_stats_base = list()
        if receiver_stats:
            group_ids = list()

            for group, stats in results:
                if group.group_type != GroupTypes.ONE_TO_ONE:
                    continue
                group_ids.append(group.group_id)

            receiver_stats_base = self.get_receiver_user_stats(group_ids, user_id, db)

        return self._group_and_stats_to_user_group_base(
            db, results, receiver_stats_base, user_id, count_unread
        )

    def get_receiver_user_stats(self, group_ids: List[str], user_id: int, db: Session):
        return (
            db.query(models.UserGroupStatsEntity)
            .filter(
                models.UserGroupStatsEntity.group_id.in_(group_ids),
                models.UserGroupStatsEntity.user_id != user_id,
            )
            .all()
        )

    def _group_and_stats_to_user_group_base(
        self,
        db: Session,
        results: List[Tuple[models.GroupEntity, models.UserGroupStatsEntity]],
        receiver_stats: List[models.UserGroupStatsEntity],
        user_id: int,
        count_unread: bool,
        count_receiver: bool = True,
    ) -> List[UserGroupBase]:
        groups = list()

        receivers = dict()
        for stat in receiver_stats:
            receivers[stat.group_id] = UserGroupStatsBase(**stat.__dict__)

        for group_entity, user_group_stats_entity in results:
            group = GroupBase(**group_entity.__dict__)
            user_group_stats = UserGroupStatsBase(**user_group_stats_entity.__dict__)

            users_join_time = self.get_user_ids_and_join_time_in_group(
                group_entity.group_id, db
            )
            user_count = len(users_join_time)

            unread_count = -1
            receiver_unread_count = -1

            if count_unread:
                # only count for receiver if it's a 1v1 group
                if group.group_type == GroupTypes.ONE_TO_ONE and count_receiver:
                    user_a, user_b = group_id_to_users(group.group_id)
                    user_to_count_for = (
                        user_a if user_b == user_id else user_b
                    )
                    receiver_unread_count = self.env.storage.get_unread_in_group(
                        group_id=group.group_id,
                        user_id=user_to_count_for,
                        last_read=user_group_stats.last_read,
                    )

                unread_count = self.env.storage.get_unread_in_group(
                    group_id=group.group_id,
                    user_id=user_id,
                    last_read=user_group_stats.last_read,
                )

            receiver_stat = None
            if group.group_id in receivers:
                receiver_stat = receivers[group.group_id]

            user_group = UserGroupBase(
                group=group,
                user_stats=user_group_stats,
                user_join_times=users_join_time,
                user_count=user_count,
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

        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(
                models.UserGroupStatsEntity.group_id == message.group_id,
                # TODO: don't filter; need to set 'last_updated_time' on every new message to sync?
                # we want to reset 'hide' and 'delete_before' when a new message is sent
                # or_(
                #     models.UserGroupStatsEntity.hide.is_(True),
                #     models.UserGroupStatsEntity.delete_before > models.UserGroupStatsEntity.join_time
                # )
            )
            .all()
        )

        for user_stat in user_stats:
            # when creating action logs, we want to sync changes to apps, but not necessarily un-hide a group
            # TODO: make we actually want to wake them up, check with stakeholders
            if wakeup_users:
                user_stat.hide = False
                user_stat.delete_before = user_stat.join_time

            user_stat.last_updated_time = sent_time
            db.add(user_stat)

        db.add(group)
        db.commit()

    def get_last_reads_in_group(self, group_id: str, db: Session) -> Dict[int, float]:
        # TODO: rethink this; some cached some not? maybe we don't have to do this twice
        users = self.get_user_ids_and_join_time_in_group(group_id, db)
        user_ids = list(users.keys())
        return self.get_last_read_in_group_for_users(group_id, user_ids, db)

    def get_last_read_in_group_for_users(
        self, group_id: str, user_ids: List[int], db: Session
    ) -> Dict[int, float]:
        not_cached = list()
        last_reads = dict()

        for user_id in user_ids:
            last_read = self.env.cache.get_last_read_in_group_for_user(
                group_id, user_id
            )

            if last_read is None:
                not_cached.append(user_id)
            else:
                last_reads[user_id] = last_read

        if len(not_cached):
            reads = (
                db.query(models.UserGroupStatsEntity)
                .with_entities(
                    models.UserGroupStatsEntity.user_id,
                    models.UserGroupStatsEntity.last_read,
                )
                .filter(
                    models.UserGroupStatsEntity.group_id == group_id,
                    models.UserGroupStatsEntity.user_id.in_(user_ids),
                )
                .all()
            )

            for user_id, last_read in reads:
                last_read_float = GroupQuery.to_ts(last_read)
                last_reads[user_id] = last_read_float

                self.env.cache.set_last_read_in_group_for_user(
                    group_id, user_id, last_read_float
                )

        return last_reads

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

    def group_exists(self, group_id: str, db: Session) -> bool:
        group = (
            db.query(literal(True))
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        return group is not None

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
        now_ts = GroupQuery.to_ts(arrow.utcnow().datetime)

        join_times = {
            user_id: GroupQuery.to_ts(stats.join_time)
            for user_id, stats in user_ids_to_stats.items()
        }
        read_times = {user_id: now_ts for user_id in user_ids}

        self.env.cache.add_user_ids_and_join_time_in_group(group_id, join_times)
        self.env.cache.set_last_read_in_group_for_users(group_id, read_times)

    def count_group_types_for_user(self, user_id: int, query: GroupQuery, db: Session) -> List[Tuple[int, int]]:
        types = self.env.cache.get_count_group_types_for_user(user_id)
        if types is not None:
            return types

        types = (
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
            )
            .group_by(
                models.GroupEntity.group_type
            )
            .limit(
                query.per_page
            )
            .all()
        )

        self.env.cache.set_count_group_types_for_user(user_id, types)
        return types

    def set_last_updated_at_on_all_stats_related_to_user(self, user_id: int, db: Session):
        now = arrow.utcnow().datetime

        group_ids = (
            db.query(models.UserGroupStatsEntity.group_id)
            .filter(
                models.UserGroupStatsEntity.user_id == user_id
            )
            .all()
        )

        before = None
        if len(group_ids) > 250:
            before = arrow.utcnow().float_timestamp

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
            the_time = arrow.utcnow().float_timestamp - before
            the_time = "%.2f" % the_time

            self.logger.info(f"updating {len(group_ids)} user group stats took {the_time}s")

    def set_last_updated_at_for_all_in_group(self, group_id: str, db: Session):
        now = arrow.utcnow().datetime

        _ = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .update({models.UserGroupStatsEntity.last_updated_time: now})
        )

        db.commit()

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

        now = arrow.utcnow().datetime

        if query.name is not None:
            group_entity.name = query.name

        if query.weight is not None:
            group_entity.group_weight = query.weight

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

        now = arrow.utcnow().datetime

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
        now = arrow.utcnow().datetime

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
        last_message_time = utc_now.datetime

        # can't be exactly the same, because when listing groups for a
        # user, any group with only one message would not be included,
        # since we're doing a filter on delete_before < last_message_time,
        # and when joining a group (happens on first message) we set
        # "delete_before = join_time = created_at"; if we set
        # last_message_time to this time as well the filter won't include
        # the group
        created_at = utc_now.shift(seconds=-1).datetime

        if query.group_type == GroupTypes.ONE_TO_ONE:
            group_id = users_to_group_id(*query.users)
        else:
            group_id = str(uuid())

        group_entity = models.GroupEntity(
            group_id=group_id,
            name=query.group_name,
            group_type=query.group_type,
            last_message_time=last_message_time,
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
        now = arrow.utcnow().datetime

        return models.UserGroupStatsEntity(
            group_id=group_id,
            user_id=user_id,
            last_read=default_dt,
            delete_before=default_dt,  # TODO: for group chats, should this be long_ago or join_time? see old history
            last_sent=default_dt,
            join_time=default_dt,
            last_updated_time=now,
            hide=False,
            pin=False,
            highlight_time=self.long_ago,
        )
