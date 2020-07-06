from sqlalchemy.orm import Session

from dinofw.rest.models import GroupQuery
from dinofw.storage.rdbms import models, schemas
from dinofw.storage.rdbms.schemas import LastReadBase, GroupBase


class RelationalHandler:
    def get_groups_for_user(self, db: Session, user_id: int, query: GroupQuery):
        results = (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.user_id == user_id)
            .filter(models.GroupEntity.last_message_time <= GroupQuery.to_dt(query.since))
            .join(models.LastReadModel, models.LastReadModel.group_id == models.GroupEntity.group_id)
            .order_by(models.GroupEntity.last_message_time.desc())
            .limit(query.per_page)
            .all()
        )

        groups = list()

        # TODO: get a list of user ids in each group as well

        for row in results:
            group = GroupBase(**row.GroupEntity)
            last_read = LastReadBase(**row.LastReadEntity)
            groups.append((group, last_read, list()))

        return groups

    def create_group(self, db: Session, group: schemas.GroupCreate):
        db_group = models.GroupEntity(**group.dict())

        db.add(db_group)
        db.commit()
        db.refresh(db_group)

        return db_group
