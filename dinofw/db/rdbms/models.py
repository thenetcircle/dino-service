from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

# from sqlalchemy import DateTime
from sqlalchemy.dialects.mysql import DATETIME

from dinofw.environ import env


class GroupEntity(env.Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), nullable=False, index=True)
    name = Column(String(128), nullable=False)

    status = Column(Integer, nullable=True)
    group_type = Column(Integer, nullable=False, server_default='0')
    last_message_time = Column(DATETIME(fsp=3), nullable=False, index=True)
    created_at = Column(DATETIME(fsp=3), nullable=False)
    owner_id = Column(Integer, nullable=False)

    updated_at = Column(DATETIME(fsp=3))
    group_meta = Column(Integer)
    group_weight = Column(Integer)
    group_context = Column(String(512))
    description = Column(String(256))
    last_message_overview = Column(String(256))


class UserGroupStatsEntity(env.Base):
    __tablename__ = "user_group_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True, nullable=False)
    user_id = Column(Integer, index=True, nullable=False)

    last_read = Column(DATETIME(fsp=3), nullable=False)
    last_sent = Column(DATETIME(fsp=3), nullable=False)
    hide_before = Column(DATETIME(fsp=3), nullable=False)
