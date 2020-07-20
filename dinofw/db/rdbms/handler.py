from datetime import datetime as dt
from typing import List, Tuple, Optional, Set, Dict
from uuid import uuid4 as uuid

import pytz
from sqlalchemy.orm import Session

from dinofw.db.cassandra.schemas import MessageBase
from dinofw.db.rdbms import models
from dinofw.db.rdbms.schemas import GroupBase
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.rest.models import AdminUpdateGroupQuery
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UpdateUserGroupStats


class RelationalHandler:
    def __init__(self, env):
        self.env = env

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = dt.utcfromtimestamp(beginning_of_1995)

    def get_users_in_group(self, group_id: str, db: Session) -> (Optional[GroupBase], Optional[Dict[int, float]]):
        group_entity = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        if group_entity is None:
            # TODO: handle
            return None, None

        group = GroupBase(**group_entity.__dict__)
        users = self.get_user_ids_and_join_times_in_group(group_id, db)

        return group, users

    def get_groups_for_user(
            self,
            user_id: int,
            query: GroupQuery,
            db: Session
    ) -> List[Tuple[GroupBase, UserGroupStatsBase, List[int]]]:
        until = GroupQuery.to_dt(query.until)
        hide_before = GroupQuery.to_dt(query.hide_before, default=self.long_ago)

        results = (
            db.query(models.GroupEntity, models.UserGroupStatsEntity)
            .join(
                models.UserGroupStatsEntity,
                models.UserGroupStatsEntity.group_id == models.GroupEntity.group_id,
            )
            .filter(
                models.GroupEntity.last_message_time <= until,
                models.GroupEntity.last_message_time > hide_before,
            )
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .order_by(models.GroupEntity.last_message_time.desc())
            .limit(query.per_page)
            .all()
        )

        groups = list()

        for group_entity, user_group_stats_entity in results:
            group = GroupBase(**group_entity.__dict__)
            user_group_stats = UserGroupStatsBase(**user_group_stats_entity.__dict__)
            users = self.get_user_ids_and_join_times_in_group(group_entity.group_id, db)

            groups.append((group, user_group_stats, users))

        return groups

    def update_group_new_message(self, message: MessageBase, sent_time: dt, db: Session) -> None:
        group = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == message.group_id)
            .first()
        )

        if group is None:
            # TODO: return error message
            return None

        group.last_message_time = sent_time
        group.last_message_overview = message.message_payload  # TODO: trim somehow, maybe has a schema

        db.add(group)
        db.commit()
        db.refresh(group)

    def remove_last_read_in_group_for_user(self, group_id: str, user_id: int, db: Session) -> None:
        _ = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .delete()
        )
        db.commit()

    def update_last_read_in_group_for_user(
            self, group_id: str, user_ids: List[int], last_read_time: dt, db: Session
    ) -> None:
        """
        TODO: should we update last read for sender? or sender also acks?
        """
        should_update_cached_user_ids_in_group = False

        for user_id in user_ids:
            user_stats = (
                db.query(models.UserGroupStatsEntity)
                .filter(models.UserGroupStatsEntity.user_id == user_id)
                .filter(models.UserGroupStatsEntity.group_id == group_id)
                .first()
            )

            if user_stats is None:
                should_update_cached_user_ids_in_group = True

                user_stats = models.UserGroupStatsEntity(
                    group_id=group_id,
                    user_id=user_id,
                    last_read=last_read_time,
                    hide_before=last_read_time,
                    last_sent=last_read_time,
                    join_time=last_read_time,
                )
            else:
                user_stats.last_read = last_read_time

            db.add(user_stats)

        db.commit()

        if should_update_cached_user_ids_in_group:
            self.get_user_ids_and_join_times_in_group(group_id, db, skip_cache=True)

    def admin_update_group_information(
            self,
            group_id: str,
            query: AdminUpdateGroupQuery,
            db: Session
    ) -> Optional[GroupBase]:
        group_entity = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        if group_entity is None:
            return None

        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)

        group_entity.status = query.group_status
        group_entity.updated_at = now

        db.add(group_entity)
        db.commit()
        db.refresh(group_entity)

        return GroupBase(**group_entity)

    def update_group_information(
            self,
            group_id: str,
            query: UpdateGroupQuery,
            db: Session
    ) -> Optional[GroupBase]:
        group_entity = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        if group_entity is None:
            return None

        now = dt.utcnow()
        now = now.replace(tzinfo=pytz.UTC)

        group_entity.name = query.group_name
        group_entity.group_weight = query.group_weight
        group_entity.group_context = query.group_context
        group_entity.updated_at = now

        db.add(group_entity)
        db.commit()
        db.refresh(group_entity)

        return GroupBase(**group_entity.__dict__)

    def get_user_stats_in_group(self, group_id: str, user_id: int, db: Session) -> Optional[UserGroupStatsBase]:
        user_stats = self.env.cache.get_user_stats_group(group_id, user_id)
        if user_stats is not None:
            return user_stats

        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        if user_stats is None:
            return None

        base = UserGroupStatsBase(**user_stats.__dict__)
        self.env.cache.set_user_stats_group(group_id, user_id, base)

        return base

    def update_user_group_stats(
            self,
            group_id: str,
            user_id: int,
            query: UpdateUserGroupStats,
            db: Session
    ) -> UserGroupStatsBase:
        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        last_read = query.last_read_time
        hide_before = query.hide_before

        should_update_cached_user_ids_in_group = False

        if user_stats is None:
            should_update_cached_user_ids_in_group = True

            if last_read is None:
                last_read = self.long_ago

            if hide_before is None:
                hide_before = self.long_ago

            user_stats = models.UserGroupStatsEntity(
                group_id=group_id,
                user_id=user_id,
                last_read=last_read,
                last_sent=self.long_ago,
                hide_before=hide_before,
                join_time=last_read,
            )

        else:
            if last_read is not None:
                user_stats.last_read = last_read

            if hide_before is not None:
                user_stats.hide_before = hide_before

        db.add(user_stats)
        db.commit()
        db.refresh(user_stats)

        base = UserGroupStatsBase(**user_stats)
        self.env.cache.set_user_stats_group(group_id, user_id, base)

        # update the cached user ids for this group (might have a new one)
        if should_update_cached_user_ids_in_group:
            self.get_user_ids_and_join_times_in_group(group_id, db, skip_cache=True)

        return base

    def update_last_read_and_sent_in_group_for_user(
            self,
            user_id: int,
            group_id: str,
            created_at: dt,
            db: Session
    ) -> None:
        user_stats = (
            db.query(models.UserGroupStatsEntity)
            .filter(models.UserGroupStatsEntity.user_id == user_id)
            .filter(models.UserGroupStatsEntity.group_id == group_id)
            .first()
        )

        should_update_cached_user_ids_in_group = False

        if user_stats is None:
            should_update_cached_user_ids_in_group = True

            user_stats = models.UserGroupStatsEntity(
                group_id=group_id,
                user_id=user_id,
                last_read=created_at,
                last_sent=created_at,
                hide_before=self.long_ago,
                join_time=created_at,
            )

        else:
            user_stats.last_read = created_at
            user_stats.last_sent = created_at

        db.add(user_stats)
        db.commit()
        db.refresh(user_stats)

        base = UserGroupStatsBase(**user_stats.__dict__)
        self.env.cache.set_user_stats_group(group_id, user_id, base)

        # update the cached user ids for this group (might have a new one)
        if should_update_cached_user_ids_in_group:
            self.get_user_ids_and_join_times_in_group(group_id, db, skip_cache=True)

    def create_group(self, owner_id: int, query: CreateGroupQuery, db: Session) -> GroupBase:
        created_at = dt.utcnow()
        created_at = created_at.replace(tzinfo=pytz.UTC)

        group_entity = models.GroupEntity(
            group_id=str(uuid()),
            name=query.group_name,
            group_type=query.group_type,
            last_message_time=created_at,
            created_at=created_at,
            owner_id=owner_id,
            group_meta=query.group_meta,
            group_context=query.group_context,
            description=query.description,
        )

        db.add(group_entity)
        db.commit()
        db.refresh(group_entity)

        return GroupBase(**group_entity.__dict__)

    def get_user_ids_and_join_times_in_group(
            self,
            group_id: str,
            db: Session,
            skip_cache: bool = False
    ) -> Dict[int, float]:
        # users in a group shouldn't change that often
        if skip_cache:
            users = None
        else:
            users = self.env.cache.get_user_ids_and_join_time_in_group(group_id)

        if users is None or len(users) == 0:
            users = (
                db.query(models.UserGroupStatsEntity)
                .filter(models.UserGroupStatsEntity.group_id == group_id)
                .distinct()
                .all()
            )
            users = {user.user_id: GroupQuery.to_ts(user.join_time) for user in users}
            self.env.cache.set_user_ids_and_join_time_in_group(group_id, users)

        return users
