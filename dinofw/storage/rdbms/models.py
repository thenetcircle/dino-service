from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import DateTime

from dinofw.environ import env


class GroupEntity(env.Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String, index=True)
    user_id = Column(Integer, index=True)

    last_message_time = Column(DateTime, index=True)
    created_at = Column(DateTime)

    name = Column(String)
    description = Column(String)
    last_message_overview = Column(String)
