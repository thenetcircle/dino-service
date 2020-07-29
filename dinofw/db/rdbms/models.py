from datetime import timezone

import sqlalchemy as sa
from sqlalchemy import Column, Boolean, DateTime, TIMESTAMP
from sqlalchemy import Integer
from sqlalchemy import String

from dinofw.environ import env


class UTCDateTime(sa.TypeDecorator):  # pylint:disable=W0223
    impl = sa.DateTime

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not value.tzinfo:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)

        return None

    def process_result_value(self, value, dialect):
        if value:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return None


class GroupEntity(env.Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True)
    name = Column(String(128))

    status = Column(Integer, nullable=True)
    group_type = Column(Integer, server_default="0")
    created_at = Column(DateTime(timezone=True))
    owner_id = Column(Integer)

    last_message_time = Column(DateTime(timezone=True), index=True)
    last_message_id = Column(String(36))
    last_message_overview = Column(String(512))

    updated_at = Column(DateTime(timezone=True))
    group_meta = Column(Integer)
    group_weight = Column(Integer)
    group_context = Column(String(512))
    description = Column(String(256))


class UserGroupStatsEntity(env.Base):
    __tablename__ = "user_group_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True)
    user_id = Column(Integer, index=True)

    last_read = Column(DateTime(timezone=True))
    last_sent = Column(DateTime(timezone=True))
    delete_before = Column(DateTime(timezone=True))
    join_time = Column(DateTime(timezone=True))

    hide = Column(Boolean())
