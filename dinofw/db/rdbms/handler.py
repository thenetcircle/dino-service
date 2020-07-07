from uuid import uuid4 as uuid
from datetime import datetime as dt
import pytz
from sqlalchemy.orm import Session

from dinofw.db.cassandra.schemas import MessageBase
from dinofw.db.rdbms.models import LastReadEntity
from dinofw.rest.models import GroupQuery, CreateGroupQuery
from dinofw.db.rdbms import models
from dinofw.db.rdbms.schemas import LastReadBase, GroupBase


class RelationalHandler:
    def __init__(self, env):
        self.env = env

    def get_groups_for_user(self, user_id: int, query: GroupQuery, db: Session):
        results = (
            db.query(models.GroupEntity, models.LastReadEntity)
            .join(
                models.LastReadEntity,
                models.LastReadEntity.group_id == models.GroupEntity.group_id,
            )
            .filter(
                models.GroupEntity.last_message_time <= GroupQuery.to_dt(query.since)
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

            # users in a group shouldn't change that often
            user_ids = self.env.cache.get_user_ids_in_group(group.group_id)

            if user_ids is None or len(user_ids) == 0:
                user_ids = (
                    db.query(models.LastReadEntity.user_id)
                    .filter(models.LastReadEntity.group_id == group.group_id)
                    .distinct()
                    .all()
                )
                user_ids = {user_id[0] for user_id in user_ids}

                self.env.cache.set_user_ids_in_group(group.group_id, user_ids)

            groups.append((group, last_read, user_ids))

        return groups

    def update_group_new_message(self, message: MessageBase, db: Session):
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

    def update_last_read_in_group_for_user(self, user_id: int, group_id: str, last_read_time: dt, db: Session):
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
