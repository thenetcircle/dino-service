from sqlalchemy.orm import Session

from dinofw.rest.models import GroupQuery
from dinofw.db.rdbms import models, schemas
from dinofw.db.rdbms.schemas import LastReadBase, GroupBase


class RelationalHandler:
    def __init__(self, env):
        self.env = env

    def get_groups_for_user(self, db: Session, user_id: int, query: GroupQuery):
        results = (
            db.query(models.GroupEntity)
            .join(
                models.LastReadModel,
                models.LastReadModel.group_id == models.GroupEntity.group_id,
            )
            .filter(
                models.GroupEntity.last_message_time <= GroupQuery.to_dt(query.since)
            )
            .filter(models.LastReadModel.user_id == user_id)
            .order_by(models.GroupEntity.last_message_time.desc())
            .limit(query.per_page)
            .all()
        )

        groups = list()

        for row in results:
            group = GroupBase(**row.GroupEntity)
            last_read = LastReadBase(**row.LastReadEntity)

            # users in a group shouldn't change that often
            user_ids = self.env.cache.get_user_ids_in_group(group.group_id)

            if user_ids is None:
                user_ids = (
                    db.query(models.LastReadModel.user_id)
                    .filter(models.LastReadModel.group_id == group.group_id)
                    .distinct()
                )
                self.env.cache.set_user_ids_in_group(group.group_id, user_ids)

            groups.append((group, last_read, user_ids))

        return groups

    def create_group(self, db: Session, group: schemas.GroupCreate):
        db_group = models.GroupEntity(**group.dict())

        db.add(db_group)
        db.commit()
        db.refresh(db_group)

        return db_group
