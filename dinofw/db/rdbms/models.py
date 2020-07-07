from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import DateTime

from dinofw.environ import env


class GroupEntity(env.Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True, nullable=False)
    name = Column(String(128), nullable=False)
    description = Column(String(256))
    last_message_overview = Column(String(256))

    last_message_time = Column(DateTime, index=True, nullable=False)
    created_at = Column(DateTime, nullable=False)


class LastReadModel(env.Base):
    __tablename__ = "last_read"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True, nullable=False)
    user_id = Column(Integer, index=True, nullable=False)
    read_at = Column(DateTime, nullable=False)
