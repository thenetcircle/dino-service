from sqlalchemy.orm import Session

from dinofw.rest.models import GroupQuery
from dinofw.storage.rdbms import models, schemas


class RelationalHandler:
    def get_groups_for_user(self, db: Session, user_id: int, query: GroupQuery):
        return (
            db.query(models.GroupEntity)
            .filter(models.GroupEntity.user_id == user_id)
            .filter(models.GroupEntity.last_message_time <= GroupQuery.to_dt(query.since))
            .order_by(models.GroupEntity.last_message_time.desc())
            .limit(query.per_page)
            .all()
        )

    def create_group(self, db: Session, group: schemas.GroupCreate):
        db_group = models.GroupEntity(**group.dict())

        db.add(db_group)
        db.commit()
        db.refresh(db_group)

        return db_group
