from datetime import datetime as dt
from typing import List, Tuple, Optional
from uuid import uuid4 as uuid

import pytz
from sqlalchemy.orm import Session

from dinofw.db.cassandra.schemas import MessageBase
from dinofw.db.rdbms import models
from dinofw.db.rdbms.models import LastReadEntity
from dinofw.db.rdbms.schemas import GroupBase, UserStatsBase
from dinofw.db.rdbms.schemas import LastReadBase
from dinofw.rest.models import CreateGroupQuery, AdminUpdateGroupQuery, UpdateGroupQuery
from dinofw.rest.models import GroupQuery


class RelationalHandler:
    def __init__(self, env):
        self.env = env

        # used when no `hide_before` is specified in a query
        beginning_of_1995 = 789_000_000
        self.long_ago = dt.utcfromtimestamp(beginning_of_1995)

    def get_users_in_group(self, group_id: str, db: Session) -> (GroupBase, List[int]):
        group_entity = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == group_id)
            .first()
        )

        group = GroupBase(**group_entity.__dict__)
        user_ids = self.get_user_ids_in_group(group_id, db)

        return group, user_ids

    def get_groups_for_user(
            self,
            user_id: int,
            query: GroupQuery,
            db: Session
    ) -> List[Tuple[GroupBase, LastReadBase, List[int]]]:
        until = GroupQuery.to_dt(query.until)
        hide_before = GroupQuery.to_dt(query.hide_before, default=self.long_ago)

        results = (
            db.query(models.GroupEntity, models.LastReadEntity)
            .join(
                models.LastReadEntity,
                models.LastReadEntity.group_id == models.GroupEntity.group_id,
            )
            .filter(
                models.GroupEntity.last_message_time <= until,
                models.GroupEntity.last_message_time > hide_before,
            )
            .filter(models.LastReadEntity.user_id == user_id)
            .order_by(models.GroupEntity.last_message_time.desc())
            .limit(query.per_page)
            .all()
        )

        groups = list()

        for group_entity, last_read_entity in results:
            group = GroupBase(**group_entity.__dict__)
            last_read = LastReadBase(**last_read_entity.__dict__)
            user_ids = self.get_user_ids_in_group(group_entity.group_id, db)

            groups.append((group, last_read, user_ids))

        return groups

    def update_group_new_message(self, message: MessageBase, db: Session) -> None:
        group = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.group_id == message.group_id)
            .first()
        )

        group.last_message_time = message.created_at
        group.last_message_overview = message.message_payload  # TODO: trim somehow, maybe has a schema

        db.add(group)
        db.commit()
        db.refresh(group)

    def update_last_read_in_group_for_user(self, user_id: int, group_id: str, last_read_time: dt, db: Session) -> None:
        """
        TODO: should we update last read for sender? or sender also acks?
        """
        last_read = (
            db.query(models.LastReadEntity)
            .filter(models.LastReadEntity.user_id == user_id)
            .filter(models.LastReadEntity.group_id == group_id)
            .first()
        )

        if last_read is None:
            last_read = LastReadEntity(
                group_id=group_id,
                user_id=user_id,
                last_read=last_read_time
            )
        else:
            last_read.last_read = last_read_time

        db.add(last_read)
        db.commit()
        db.refresh(last_read)

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

        return GroupBase(**group_entity)

    def get_user_stats_in_group(self, group_id: str, user_id: int, db: Session) -> Optional[UserStatsBase]:
        user_stats = self.env.cache.get_user_stats_group(group_id, user_id)
        if user_stats is not None:
            return UserStatsBase(**user_stats)

        user_stats = (
            db.query(models.UserStatsEntity)
            .filter(models.UserStatsEntity.user_id == user_id)
            .filter(models.UserStatsEntity.group_id == group_id)
            .first()
        )

        if user_stats is None:
            return None

        base = UserStatsBase(**user_stats)
        self.env.cache.set_user_stats_group(group_id, user_id, base)

        return base

    def create_group(self, user_id: int, query: CreateGroupQuery, db: Session) -> GroupBase:
        created_at = dt.utcnow()
        created_at = created_at.replace(tzinfo=pytz.UTC)

        group_entity = models.GroupEntity(
            group_id=str(uuid()),
            name=query.group_name,
            group_type=query.group_type,
            last_message_time=created_at,
            created_at=created_at,
            owner_id=user_id,
            group_meta=query.group_meta,
            group_context=query.group_context,
            description=query.description,
        )

        db.add(group_entity)
        db.commit()
        db.refresh(group_entity)

        return GroupBase(**group_entity.__dict__)

    def get_user_ids_in_group(self, group_id: str, db: Session) -> List[int]:
        # users in a group shouldn't change that often
        user_ids = self.env.cache.get_user_ids_in_group(group_id)

        if user_ids is None or len(user_ids) == 0:
            user_ids = (
                db.query(models.LastReadEntity.user_id)
                .filter(models.LastReadEntity.group_id == group_id)
                .distinct()
                .all()
            )
            user_ids = {user_id[0] for user_id in user_ids}

            self.env.cache.set_user_ids_in_group(group_id, user_ids)

        return user_ids
