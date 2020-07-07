from sqlalchemy.orm import Session

from dinofw.db.cassandra.schemas import MessageBase
from dinofw.rest.models import GroupQuery
from dinofw.db.rdbms import models, schemas
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

    def update_last_read_on_send_new_message(self, user_id: int, message: MessageBase, db: Session):
        """
        TODO: should we update last read for sender? or sender also acks?
        """
        last_read = (
            db.query(models.LastReadEntity)
            .filter(models.LastReadEntity.user_id == user_id)
            .filter(models.LastReadEntity.group_id == message.group_id)
            .first()
        )

        last_read.last_read = message.created_at

        db.add(last_read)
        db.commit()
        db.refresh(last_read)

    def create_group(self, db: Session, group: schemas.GroupCreate):
        db_group = models.GroupEntity(**group.dict())

        db.add(db_group)
        db.commit()
        db.refresh(db_group)

        return db_group
