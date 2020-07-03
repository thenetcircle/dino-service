from sqlalchemy.orm import Session

from dinofw.storage.rdbms import models, schemas


def get_groups_for_user(db: Session, user_id: int):
    return (
        db.query(models.GroupEntity)
        .filter(models.GroupEntity.user_id == user_id)
        .order()
        .all()
    )


def get_refinements(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.GroupEntity).offset(skip).limit(limit).all()


def create_refinement(db: Session, group: schemas.GroupCreate):
    db_group = models.GroupEntity(**group.dict())

    db.add(db_group)
    db.commit()
    db.refresh(db_group)

    return db_group
