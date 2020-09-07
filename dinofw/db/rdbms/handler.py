import logging
from datetime import datetime as dt
from typing import List, Optional, Dict
from uuid import uuid4 as uuid

import arrow
from sqlalchemy import literal
from sqlalchemy.orm import Session

from dinofw.config import GroupTypes
from dinofw.db.rdbms import models
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.rdbms.schemas import UserGroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.db.storage.schemas import MessageBase
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UpdateUserGroupStats
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import UserNotInGroupException


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
            raise NoSuchGroupException(f"no such group: {group_id}")

        group = GroupBase(**group_entity.__dict__)
        users_and_join_time = self.get_user_ids_and_join_time_in_group(group_id, db)
        user_count = self.count_users_in_group(group_id, db)

        return group, users_and_join_time, user_count

    def get_groups_for_user(
        self,
        user_id: int,
        query: GroupQuery,
        db: Session,
        count_users: bool = True,
        count_unread: bool = True,
    ) -> List[UserGroupBase]:
        """
        what we're doing:

        select * from
            groups g
        inner join
            user_group_stats u on u.group_id = g.group_id
        where
            user_id = 1234 and
            u.hide = false
        order by
            u.highlight_time desc,
            u.pin desc,
            g.last_message_time desc
        """
        until = GroupQuery.to_dt(query.until)

        results = (
            db.query(models.GroupEntity, models.UserGroupStatsEntity)
            .join(
                models.UserGroupStatsEntity,
                models.UserGroupStatsEntity.group_id == models.GroupEntity.group_id,
            )
            .filter(
                models.GroupEntity.last_message_time <= until,
                models.UserGroupStatsEntity.hide.is_(False),
                models.UserGroupStatsEntity.user_id == user_id,
            )
            .order_by(
                models.UserGroupStatsEntity.highlight_time.desc(),
                models.UserGroupStatsEntity.pin.desc(),
                models.GroupEntity.last_message_time.desc(),
            )
            .limit(query.per_page)
            .all()
        )

        groups = list()

        for group_entity, user_group_stats_entity in results:
            group = GroupBase(**group_entity.__dict__)
            user_group_stats = UserGroupStatsBase(**user_group_stats_entity.__dict__)

            users_join_time = self.get_user_ids_and_join_time_in_group(group_entity.group_id, db)

            if count_unread:
                unread_count = self.env.storage.get_unread_in_group(
                    group_id=group.group_id,
                    user_id=user_id,
                    last_read=user_group_stats.last_read
                )
            else:
                unread_count = 0

            if count_users:
                user_count = self.count_users_in_group(group_entity.group_id, db)
            else:
                user_count = 0

            user_group = UserGroupBase(
                group=group,
                user_stats=user_group_stats,
                user_join_times=users_join_time,
                user_count=user_count,
                unread_count=unread_count
            )
            groups.append(user_group)

        return groups

    def update_group_new_message(
        self, message: MessageBase, sent_time: dt, db: Session
    ) -> None:
        group = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == message.group_id)
            .first()
        )

        if group is None:
            raise NoSuchGroupException(f"no such group: {message.group_id}")

        group.last_message_time = sent_time
        group.last_message_overview = message.message_payload
        group.last_message_id = message.message_id
        group.last_message_type = message.message_type

        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(
                models.UserGroupStatsEntity.group_id == message.group_id,
                models.UserGroupStatsEntity.hide.is_(True),
            )
            .all()
        )

        for user_stat in user_stats:
            user_stat.hide = False
            db.add(user_stat)

        db.add(group)
        db.commit()

    def get_last_reads_in_group(self, group_id: str, db: Session) -> Dict[int, float]:
        # TODO: rethink this; some cached some not? maybe we don't have to do this twice
        users = self.get_user_ids_and_join_time_in_group(group_id, db)
        user_ids = list(users.keys())
        return self.get_last_read_in_group_for_users(group_id, user_ids, db)

    def get_last_read_in_group_for_users(self, group_id: str, user_ids: List[int], db: Session) -> Dict[int, float]:
        not_cached = list()
        last_reads = dict()

        for user_id in user_ids:
            last_read = self.env.cache.get_last_read_in_group_for_user(group_id, user_id)

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
                    models.UserGroupStatsEntity.user_id.in_(user_ids)
                )
                .all()
            )

            for user_id, last_read in reads:
                last_read_float = GroupQuery.to_ts(last_read)
                last_reads[user_id] = last_read_float

                self.env.cache.set_last_read_in_group_for_user(group_id, user_id, last_read_float)

        return last_reads

    def get_group_id_for_1to1(self, user_a: int, user_b: int, db: Session) -> Optional[str]:
        group = self.get_group_for_1to1(user_a, user_b, db)

        if group is None:
            raise NoSuchGroupException(",".join([str(user_id) for user_id in [user_a, user_b]]))

        return group.group_id

    def get_group_for_1to1(self, user_a: int, user_b: int, db: Session) -> Optional[GroupBase]:
        users = sorted([user_a, user_b])

        group = (
            db.query(
                models.GroupEntity
            )
            .filter(
                models.GroupEntity.group_type == GroupTypes.ONE_TO_ONE,
                models.GroupEntity.user_a == users[0],
                models.GroupEntity.user_b == users[1],
            )
            .first()
        )

        if group is None:
            raise NoSuchGroupException(",".join([str(user_id) for user_id in users]))

        return GroupBase(**group.__dict__)

    def create_group_for_1to1(self, user_a: int, user_b: int, db: Session) -> GroupBase:
        users = sorted([user_a, user_b])
        group_name = ",".join([str(user_id) for user_id in users])

        query = CreateGroupQuery(
            group_name=group_name,
            group_type=GroupTypes.ONE_TO_ONE,
            users=users
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

        user_ids_join_time = {
            user[0]: GroupQuery.to_ts(user[1])
            for user in users
        }

        self.env.cache.set_user_ids_and_join_time_in_group(
            group_id,
            user_ids_join_time
        )

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
                user_ids_for_cache.add(user_id)
                user_ids_to_stats[user_id] = self._create_user_stats(group_id, user_id, now)

            user_ids_to_stats[user_id].last_read = now
            db.add(user_ids_to_stats[user_id])

        db.commit()
        now_ts = GroupQuery.to_ts(arrow.utcnow().datetime)

        join_times = {
            user_id: GroupQuery.to_ts(stats.join_time)
            for user_id, stats in user_ids_to_stats.items()
        }
        read_times = {
            user_id: now_ts
            for user_id in user_ids
        }

        self.env.cache.add_user_ids_and_join_time_in_group(group_id, join_times)
        self.env.cache.set_last_read_in_group_for_users(group_id, read_times)

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
        highlight_time = UpdateUserGroupStats.to_dt(query.highlight_time, allow_none=True)
        now = arrow.utcnow().datetime

        if user_stats is None:
            raise UserNotInGroupException(
                f"tried to update group stats for user {user_id} not in group {group_id}"
            )

        # only update if query has new values
        else:
            # used by apps to sync changes
            user_stats.last_updated_time = now

            if last_read is not None:
                user_stats.last_read = last_read

                # highlight time is removed if a user reads a conversation
                user_stats.highlight_time = self.long_ago

            if delete_before is not None:
                user_stats.delete_before = delete_before

            if query.pin is not None:
                user_stats.pin = query.pin

            if query.rating is not None:
                user_stats.rating = query.rating

            # can't set highlight time if also setting last read time
            if highlight_time is not None and last_read is None:
                user_stats.highlight_time = highlight_time
                user_stats.last_updated_time = now

                # always becomes unhidden if highlighted
                user_stats.hide = False
                self.env.cache.set_hide_group(group_id, False, [user_id])

            elif query.hide is not None:
                user_stats.hide = query.hide
                self.env.cache.set_hide_group(group_id, query.hide, [user_id])

        db.add(user_stats)
        db.commit()

    def update_last_read_and_highlight_in_group_for_user(
            self,
            group_id: str,
            user_id: int,
            the_time: dt,
            db: Session
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

        self.env.cache.set_last_read_in_group_for_user(group_id, user_id, GroupQuery.to_ts(the_time))
        self.env.cache.set_hide_group(group_id, False)
        self.env.cache.set_unread_in_group(group_id, user_id, 0)

        if user_stats is None:
            raise UserNotInGroupException(f"user {user_id} is not in group {group_id}")

        user_stats.last_read = the_time
        user_stats.last_sent = the_time
        user_stats.last_updated_time = the_time

        if user_stats.first_sent is None:
            user_stats.first_sent = the_time

        db.add(user_stats)
        db.commit()

    def create_group(
        self, owner_id: int, query: CreateGroupQuery, db: Session
    ) -> GroupBase:
        created_at = arrow.utcnow().datetime

        user_a, user_b = None, None

        if query.group_type == GroupTypes.ONE_TO_ONE:
            users = sorted(query.users)
            user_a = users[0]
            user_b = users[1]

        group_entity = models.GroupEntity(
            group_id=str(uuid()),
            name=query.group_name,
            user_a=user_a,
            user_b=user_b,
            group_type=query.group_type,
            last_message_time=created_at,
            updated_at=created_at,
            created_at=created_at,
            owner_id=owner_id,
            meta=query.meta,
            context=query.context,
            weight=query.weight,
            description=query.description,
        )

        for user_id in query.users:
            user_stats = self._create_user_stats(group_entity.group_id, user_id, created_at)
            db.add(user_stats)

        base = GroupBase(**group_entity.__dict__)

        db.add(group_entity)
        db.commit()

        return base

    def count_users_in_group(self, group_id: str, db: Session) -> int:
        user_count = self.env.cache.get_user_count_in_group(group_id)
        if user_count is not None:
            return user_count

        user_count = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .distinct()
            .count()
        )

        return user_count

    def update_user_message_status(self, user_id: int, query: MessageQuery, db: Session) -> None:
        user_stats = (
            db.query(models.UserStatsEntity)
            .filter(
                models.UserGroupStatsEntity.user_id == user_id,
            )
            .first()
        )

        if user_stats is None:
            user_stats = models.UserStatsEntity(
                user_id=user_id,
                status=query.status
            )
        else:
            user_stats.status = query.status

        db.add(user_stats)
        db.commit()

        self.env.cache.update_user_message_status(user_id, query.status)

        # TODO: need to publish a message to all online users this user has contacted before...

    def _get_user_stats_for(self, group_id: str, user_id: int, db: Session):
        return (
            db.query(models.UserGroupStatsEntity)
            .filter(
                models.UserGroupStatsEntity.group_id == group_id,
                models.UserGroupStatsEntity.user_id == user_id,
            )
            .first()
        )

    def _create_user_stats(self, group_id: str, user_id: int, default_dt: dt) -> models.UserGroupStatsEntity:
        now = arrow.utcnow().datetime

        return models.UserGroupStatsEntity(
            group_id=group_id,
            user_id=user_id,
            last_read=default_dt,
            delete_before=default_dt,
            last_sent=default_dt,
            join_time=default_dt,
            last_updated_time=now,
            hide=False,
            pin=False,
            highlight_time=self.long_ago,
        )
