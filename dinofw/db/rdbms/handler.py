from datetime import datetime as dt
from typing import List, Tuple, Optional, Dict
from uuid import uuid4 as uuid

import arrow
from sqlalchemy.orm import Session

from dinofw.db.storage.schemas import MessageBase
from dinofw.db.rdbms import models
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.rest.server.models import AdminUpdateGroupQuery
from dinofw.rest.server.models import CreateGroupQuery
from dinofw.rest.server.models import GroupQuery
from dinofw.rest.server.models import UpdateGroupQuery
from dinofw.rest.server.models import UpdateUserGroupStats


class RelationalHandler:
    def __init__(self, env):
        self.env = env

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = dt.utcfromtimestamp(beginning_of_1995)

    def get_users_in_group(
        self, group_id: str, query: GroupQuery, db: Session
    ) -> (Optional[GroupBase], Optional[Dict[int, float]], Optional[int]):
        group_entity = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        if group_entity is None:
            # TODO: handle
            return None, None, None

        group = GroupBase(**group_entity.__dict__)
        users = self.get_user_ids_and_join_times_in_group(group_id, query, db)
        user_count = self.count_users_in_group(group_id, db)

        return group, users, user_count

    def get_groups_for_user(
        self, user_id: int, query: GroupQuery, db: Session, count_users: bool = True
    ) -> List[Tuple[GroupBase, UserGroupStatsBase, Dict[int, float], int]]:
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
                models.UserGroupStatsEntity.pin.desc(),
                models.GroupEntity.last_message_time.desc()
            )
            .limit(query.per_page)
            .all()
        )

        groups = list()
        sub_query = GroupQuery(per_page=50)

        for group_entity, user_group_stats_entity in results:
            group = GroupBase(**group_entity.__dict__)
            user_group_stats = UserGroupStatsBase(**user_group_stats_entity.__dict__)

            users = self.get_user_ids_and_join_times_in_group(
                group_entity.group_id, sub_query, db
            )

            if count_users:
                user_count = self.count_users_in_group(group_entity.group_id, db)
            else:
                user_count = 0

            groups.append((group, user_group_stats, users, user_count))

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
            # TODO: return error message
            return None

        group.last_message_time = sent_time
        group.last_message_overview = message.message_payload
        group.last_message_id = message.message_id

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
        user_ids = self.get_user_ids_in_group(group_id, db)
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

    def get_user_ids_in_group(self, group_id: str, db: Session):
        users = self.env.cache.get_user_ids_and_join_time_in_group(group_id)

        if users is None:
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

            user_ids = [user[0] for user in users]
        else:
            user_ids = list(users.keys())

        return user_ids

    def remove_last_read_in_group_for_user(
        self, group_id: str, user_id: int, db: Session
    ) -> None:
        _ = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .delete()
        )
        db.commit()

        self.env.cache.remove_last_read_in_group_for_user(group_id, user_id)

    def group_exists(self, group_id: str, db: Session) -> bool:
        group = (
            db.query(models.GroupEntity)
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
        """
        TODO: should we update last read for sender? or sender also acks?
        """
        user_ids_for_cache = set()

        for user_id, join_time in users.items():
            user_stats = (
                db.query(models.UserGroupStatsEntity)
                .filter(models.UserGroupStatsEntity.user_id == user_id)
                .filter(models.UserGroupStatsEntity.group_id == group_id)
                .first()
            )

            if user_stats is None:
                user_ids_for_cache.add(user_id)

                user_stats = models.UserGroupStatsEntity(
                    group_id=group_id,
                    user_id=user_id,
                    last_read=now,
                    delete_before=now,
                    last_sent=now,
                    join_time=now,
                    hide=False,
                )
            else:
                user_stats.last_read = now

            db.add(user_stats)

        db.commit()

        now_ts = GroupQuery.to_ts(now)
        for user_id in user_ids_for_cache:
            self.env.cache.set_last_read_in_group_for_user(group_id, user_id, now_ts)

    def admin_update_group_information(
        self, group_id: str, query: AdminUpdateGroupQuery, db: Session
    ) -> Optional[GroupBase]:
        group_entity = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        if group_entity is None:
            return None

        now = arrow.utcnow().datetime

        group_entity.status = query.group_status
        group_entity.updated_at = now

        base = GroupBase(**group_entity)

        db.add(group_entity)
        db.commit()

        return base

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

        group_entity.name = query.group_name
        group_entity.group_weight = query.group_weight
        group_entity.group_context = query.group_context
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
            return None

        base = UserGroupStatsBase(**user_stats.__dict__)

        return base

    def update_user_group_stats(
        self, group_id: str, user_id: int, query: UpdateUserGroupStats, db: Session
    ) -> UserGroupStatsBase:
        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        last_read = UpdateUserGroupStats.to_dt(query.last_read_time, allow_none=True)
        delete_before = UpdateUserGroupStats.to_dt(query.delete_before, allow_none=True)

        if user_stats is None:
            user_stats = models.UserGroupStatsEntity(
                group_id=group_id,
                user_id=user_id,
                last_sent=self.long_ago,
                hide_before=self.long_ago,
                last_read=last_read or self.long_ago,
                delete_before=delete_before or self.long_ago,
                hide=query.hide or False,
                join_time=last_read or self.long_ago,
                pin=query.pin or False,
            )

        # only update if query has new values
        else:
            if last_read is not None:
                user_stats.last_read = last_read

            if delete_before is not None:
                user_stats.delete_before = delete_before

            if query.hide is not None:
                user_stats.hide = query.hide
                self.env.cache.set_hide_group(group_id, query.hide, [user_id])

            if query.pin is not None:
                user_stats.pin = query.pin

        base = UserGroupStatsBase(
            group_id=user_stats.group_id,
            user_id=user_stats.user_id,
            last_read=user_stats.last_read,
            last_sent=user_stats.last_sent,
            delete_before=user_stats.delete_before,
            join_time=user_stats.join_time,
            hide=user_stats.hide,
            bookmark=user_stats.bookmark,
            pin=user_stats.pin,
        )

        db.add(user_stats)
        db.commit()

        return base

    def update_last_read_and_sent_in_group_for_user(
        self, user_id: int, group_id: str, created_at: dt, db: Session
    ) -> None:
        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        self.env.cache.increase_unread_in_group(group_id)
        self.env.cache.set_last_read_in_group_for_user(group_id, user_id, GroupQuery.to_ts(created_at))
        self.env.cache.set_hide_group(group_id, False)
        self.env.cache.set_unread_in_group(group_id, user_id, 0)

        if user_stats is None:
            user_stats = models.UserGroupStatsEntity(
                group_id=group_id,
                user_id=user_id,
                last_read=created_at,
                last_sent=created_at,
                delete_before=self.long_ago,
                join_time=created_at,
            )

        else:
            user_stats.last_read = created_at
            user_stats.last_sent = created_at

        db.add(user_stats)
        db.commit()

    def create_group(
        self, owner_id: int, query: CreateGroupQuery, db: Session
    ) -> GroupBase:
        created_at = arrow.utcnow().datetime

        group_entity = models.GroupEntity(
            group_id=str(uuid()),
            name=query.group_name,
            group_type=query.group_type,
            last_message_time=created_at,
            updated_at=created_at,
            created_at=created_at,
            owner_id=owner_id,
            group_meta=query.group_meta,
            group_context=query.group_context,
            description=query.description,
        )

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

    def get_user_ids_and_join_times_in_group(
        self, group_id: str, query: GroupQuery, db: Session, skip_cache: bool = False
    ) -> Dict[int, float]:
        until = GroupQuery.to_dt(query.until)

        # TODO: since we're doing pagination instead, maybe just skip caching? how often will people check?
        users = None
        # users in a group shouldn't change that often
        # if skip_cache:
        #     users = None
        # else:
        #     users = self.env.cache.get_user_ids_and_join_time_in_group(group_id)

        if users is None:  # or len(users) == 0:
            users = (
                db.query(models.UserGroupStatsEntity)
                .filter(
                    models.UserGroupStatsEntity.group_id == group_id,
                    models.UserGroupStatsEntity.join_time <= until,
                )
                .distinct()
                .order_by(models.UserGroupStatsEntity.join_time.desc())
                .limit(query.per_page or 50)
                .all()
            )

            users = {user.user_id: GroupQuery.to_ts(user.join_time) for user in users}
            # self.env.cache.set_user_ids_and_join_time_in_group(group_id, users)

        return users
